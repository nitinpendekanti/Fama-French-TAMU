import pandas as pd
import numpy as np
import sqlite3
import statsmodels.formula.api as smf
from regtabletotext import prettify_result

tidy_finance = sqlite3.connect(
    database="../data/tidy_finance.sqlite"
)

crsp_monthly = (pd.read_sql_query(
    sql=("SELECT permno, gvkey, date, month, ret_excess, mktcap, "
         "mktcap_lag, exchange FROM crsp_monthly"),
    con=tidy_finance,
    parse_dates={"date", "month"},)
    .dropna()
)



compustat = (pd.read_sql_query(
    sql="SELECT gvkey, datadate, be, op, inv FROM compustat",
    con=tidy_finance,
    parse_dates={"datadate"})
    .dropna()
)


factors_ff3_monthly = pd.read_sql_query(
    sql="SELECT date, smb, hml FROM factors_ff3_monthly",
    con=tidy_finance,
    parse_dates={"date"}
)



factors_ff5_monthly = pd.read_sql_query(
    sql="SELECT date, smb, hml, rmw, cma FROM factors_ff5_monthly",
    con=tidy_finance,
    parse_dates={"date"}
)
print(crsp_monthly['date'].tail(20))

tmp = (crsp_monthly
    .query("date.dt.month == 6"))
print(crsp_monthly['date'].dt.month.value_counts())

print(tmp)

size = (crsp_monthly
    .query("date.dt.month == 6")
    .assign(sorting_date=lambda x: (x["date"]+pd.DateOffset(months=1)))
    .get(["permno", "exchange", "sorting_date", "mktcap"])
    .rename(columns={"mktcap": "size"})
)



market_equity = (crsp_monthly
    .query("date.dt.month == 12")
    .assign(sorting_date=lambda x: (x["date"]+pd.DateOffset(months=7)))
    .get(["permno", "gvkey", "sorting_date", "mktcap"])
    .rename(columns={"mktcap": "me"})
)

book_to_market = (compustat
    .assign(
        sorting_date=lambda x: (pd.to_datetime(
            (x["datadate"].dt.year+1).astype(str)+"0701", format="%Y%m%d")
        )
    )
    .merge(market_equity, how="inner", on=["gvkey", "sorting_date"])
    .assign(bm=lambda x: x["be"]/x["me"])
    .get(["permno", "sorting_date", "me", "bm"])
)


sorting_variables = (size
    .merge(book_to_market, how="inner", on=["permno", "sorting_date"])
    .dropna()
    .drop_duplicates(subset=["permno", "sorting_date"])
)


def assign_portfolio(data, sorting_variable, percentiles):
    """Assign portfolios to a bin according to a sorting variable."""
    breakpoints = (data
        .query("exchange == 'NYSE'")
        .get(sorting_variable)
        .quantile(percentiles, interpolation="linear")
    )
    breakpoints.iloc[0] = -np.inf
    breakpoints.iloc[breakpoints.size-1] = np.inf
    assigned_portfolios = pd.cut(
        data[sorting_variable],
        bins=breakpoints,
        labels=pd.Series(range(1, breakpoints.size)),
        include_lowest=True,
        right=False
    )
    return assigned_portfolios

portfolios = (sorting_variables
    .groupby("sorting_date")
    .apply(lambda x: x
        .assign(
            portfolio_size=assign_portfolio(x, "size", [0, 0.5, 1]),
            portfolio_bm=assign_portfolio(x, "bm", [0, 0.3, 0.7, 1])
        )
    )
    .reset_index(drop=True)
    .get(["permno", "sorting_date", "portfolio_size", "portfolio_bm"])
)

portfolios = (crsp_monthly
    .assign(
        sorting_date=lambda x: (pd.to_datetime(
            x["date"].apply(lambda x: str(x.year-1)+
                         "0701" if x.month <= 6 else str(x.year)+"0701")))
    )
    .merge(portfolios, how="inner", on=["permno", "sorting_date"])
)

factors_replicated = (portfolios
    .groupby(["portfolio_size", "portfolio_bm", "date"])
    .apply(lambda x: pd.Series({
        "ret": np.average(x["ret_excess"], weights=x["mktcap_lag"])
    })
    )
    .reset_index()
    .groupby("date")
    .apply(lambda x: pd.Series({
        "smb_replicated": (
            x["ret"][x["portfolio_size"] == 1].mean() -
            x["ret"][x["portfolio_size"] == 2].mean()),
        "hml_replicated": (
            x["ret"][x["portfolio_bm"] == 3].mean() -
            x["ret"][x["portfolio_bm"] == 1].mean())
    }))
    .reset_index()
)

start_date = pd.to_datetime("01/01/1960", format="%m/%d/%Y")
end_date = pd.to_datetime("12/31/2023", format="%m/%d/%Y")

factors_replicated = factors_replicated[(factors_replicated['date'] >= start_date) & (factors_replicated['date'] <= end_date)]
factors_ff3_monthly = factors_ff3_monthly[(factors_ff3_monthly['date'] >= start_date) & (factors_ff3_monthly['date'] <= end_date)]

factors_replicated = (factors_replicated
    .merge(factors_ff3_monthly, how="inner", on="date")
    .round(4)
)

model_smb = (smf.ols(
    formula="smb ~ smb_replicated", 
    data=factors_replicated
  )
  .fit()
)
prettify_result(model_smb)

model_hml = (smf.ols(
    formula="hml ~ hml_replicated", 
    data=factors_replicated
  )
  .fit()
)
prettify_result(model_hml)