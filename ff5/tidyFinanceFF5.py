import pandas as pd
import numpy as np
import sqlite3
import statsmodels.formula.api as smf
from regtabletotext import prettify_result

tidy_finance = sqlite3.connect(
    database="../data/fama_french_data.sqlite"
)

crsp_monthly = (pd.read_sql_query(
    sql=("SELECT permno, gvkey, date, ret_excess, mktcap, "
         "mktcap_lag, exchange FROM crsp_monthly"),
    con=tidy_finance,
    parse_dates={"date"})
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

other_sorting_variables = (compustat
  .assign(
    sorting_date=lambda x: (pd.to_datetime(
      (x["datadate"].dt.year+1).astype(str)+"0701", format="%Y%m%d")
    )
  )
  .merge(market_equity, how="inner", on=["gvkey", "sorting_date"])
  .assign(bm=lambda x: x["be"]/x["me"])
  .get(["permno", "sorting_date", "me", "bm", "op", "inv"])
)

sorting_variables = (size
  .merge(other_sorting_variables, how="inner", on=["permno", "sorting_date"])
  .dropna()
  .drop_duplicates(subset=["permno", "sorting_date"])
)

portfolios = (sorting_variables
  .groupby("sorting_date")
  .apply(lambda x: x
    .assign(
      portfolio_size=assign_portfolio(x, "size", [0, 0.5, 1])
    )
  )
  .reset_index(drop=True)
  .groupby(["sorting_date", "portfolio_size"])
  .apply(lambda x: x
    .assign(
      portfolio_bm=assign_portfolio(x, "bm", [0, 0.3, 0.7, 1]),
      portfolio_op=assign_portfolio(x, "op", [0, 0.3, 0.7, 1]),
      portfolio_inv=assign_portfolio(x, "inv", [0, 0.3, 0.7, 1])
    )
  )
  .reset_index(drop=True)
  .get(["permno", "sorting_date", 
        "portfolio_size", "portfolio_bm",
        "portfolio_op", "portfolio_inv"])
)

portfolios = (crsp_monthly
  .assign(
    sorting_date=lambda x: (pd.to_datetime(
      x["date"].apply(lambda x: str(x.year-1)+
        "0701" if x.month <= 6 else str(x.year)+"0701")))
  )
  .merge(portfolios, how="inner", on=["permno", "sorting_date"])
)

portfolios_value = (portfolios
  .groupby(["portfolio_size", "portfolio_bm", "date"])
  .apply(lambda x: pd.Series({
      "ret": np.average(x["ret_excess"], weights=x["mktcap_lag"])
    })
  )
  .reset_index()
)

factors_value = (portfolios_value
  .groupby("date")
  .apply(lambda x: pd.Series({
    "hml_replicated": (
      x["ret"][x["portfolio_bm"] == 3].mean() - 
        x["ret"][x["portfolio_bm"] == 1].mean())})
  )
  .reset_index()
)

portfolios_profitability = (portfolios
  .groupby(["portfolio_size", "portfolio_op", "date"])
  .apply(lambda x: pd.Series({
      "ret": np.average(x["ret_excess"], weights=x["mktcap_lag"])
    })
  )
  .reset_index()
)

factors_profitability = (portfolios_profitability
  .groupby("date")
  .apply(lambda x: pd.Series({
    "rmw_replicated": (
      x["ret"][x["portfolio_op"] == 3].mean() - 
        x["ret"][x["portfolio_op"] == 1].mean())})
  )
  .reset_index()
)

portfolios_investment = (portfolios
  .groupby(["portfolio_size", "portfolio_inv", "date"])
  .apply(lambda x: pd.Series({
      "ret": np.average(x["ret_excess"], weights=x["mktcap_lag"])
    })
  )
  .reset_index()
)

factors_investment = (portfolios_investment
  .groupby("date")
  .apply(lambda x: pd.Series({
    "cma_replicated": (
      x["ret"][x["portfolio_inv"] == 1].mean() - 
        x["ret"][x["portfolio_inv"] == 3].mean())})
  )
  .reset_index()
)

factors_size = (
  pd.concat(
    [portfolios_value, portfolios_profitability, portfolios_investment], 
    ignore_index=True
  )
  .groupby("date")
  .apply(lambda x: pd.Series({
    "smb_replicated": (
      x["ret"][x["portfolio_size"] == 1].mean() - 
        x["ret"][x["portfolio_size"] == 2].mean())})
  )
  .reset_index()
)

factors_replicated = (factors_size
  .merge(factors_value, how="outer", on="date")
  .merge(factors_profitability, how="outer", on="date")
  .merge(factors_investment, how="outer", on="date")
)

factors_replicated = (factors_replicated
  .merge(factors_ff5_monthly, how="inner", on="date")
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

model_rmw = (smf.ols(
    formula="rmw ~ rmw_replicated", 
    data=factors_replicated
  )
  .fit()
)
prettify_result(model_rmw)

model_cma = (smf.ols(
    formula="cma ~ cma_replicated", 
    data=factors_replicated
  )
  .fit()
)
prettify_result(model_cma)