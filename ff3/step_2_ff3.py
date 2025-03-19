""" Fama French 3 Step 2 Replication """

import sys
import os
import joblib
import numpy as np

sys.path.append(os.path.abspath(".."))
from database_connector import DatabaseConnector
import pandas as pd

# If set to true, use the cached queries, else it will requery the sqlite database using DatabaseConnector
useCache: bool = True

# If set to true, connects to sqlite database (must be set to True if useCache is False)
useDC: bool = False

# Create Connection to Database and Get Access to Tables (redo this when database changes)
if useDC:
  dc = DatabaseConnector(f"sqlite:///{os.path.abspath('../data/fama_french_data.sqlite')}")

"""Get Market Equity for SMB"""
if useCache:
    marketEquitySMB = joblib.load("../data/marketEquitySMBFF3Step2.joblib")
else:
  marketEquitySMB = (dc.crspMonthly
    .assign(sorting_date=lambda x: (x["date"]+pd.DateOffset(months=1)))
    .get(["permno", "exchange", "sorting_date", "mktcap"])
    .rename(columns={"mktcap": "size"})
  )
  joblib.dump(marketEquitySMB, "../data/marketEquitySMBFF3Step2.joblib")

"""Get Market Equity for HML"""
if useCache:
  marketEquityHML = joblib.load("../data/marketEquityHMLFF3Step2.joblib")
else:
  marketEquityHML = (dc.crspMonthly
    .assign(sorting_date=lambda x: (x["date"]+pd.DateOffset(months=1)))
    .get(["permno", "gvkey", "sorting_date", "mktcap"])
    .rename(columns={"mktcap": "me"})
  )
  joblib.dump(marketEquityHML, "../data/marketEquityHMLFF3Step2.joblib")

"""Get Market Equity for HML"""
if useCache:
  bookToMarket = joblib.load("../data/bookToMarketFF3Step2.joblib")
else:
  bookToMarket = (dc.compustat
    .assign(
      sorting_date=lambda x: (pd.to_datetime(
        (x["datadate"].dt.year+1).astype(str)+"0701", format="%Y%m%d")
      )
    )
    .merge(marketEquityHML, how="inner", on=["gvkey", "sorting_date"])
    .assign(bm=lambda x: x["be"]/x["me"])
    .get(["permno", "datadate", "sorting_date" ,"me", "bm"])
  )
  joblib.dump(bookToMarket, "../data/bookToMarketFF3Step2.joblib")

"""Merge Market Equity (SMB) and Book-To-Market (HML) Into one table"""
# .query("shrcd in [10, 11]")
sorting_variables = (marketEquitySMB
  .merge(bookToMarket, how="inner", on=["permno", "sorting_date"])
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

"""Split NYSE Stocks into 2 size bins and 3 bm bins"""
if useCache:
  portfolios = joblib.load("../data/portfolios-step-2-ff3.joblib")
else:
  annualPortfolios = (sorting_variables
    .groupby("sorting_date")
    .apply(lambda portfolio: portfolio
      .assign(
        portfolio_size=assign_portfolio(portfolio, "size", [0, 0.5, 1]),
        portfolio_bm=assign_portfolio(portfolio, "bm", [0, 0.3, 0.7, 1])
      )
    )
    .reset_index(drop=True)
    .get(["permno", "sorting_date", "portfolio_size", "portfolio_bm"])
  )

  portfolios = (dc.crspMonthly
    .assign(
      sorting_date=lambda x: x["date"].apply(
        lambda d: d + pd.DateOffset(months=1) - pd.DateOffset(days=d.day-1)
      )
    )
    .pipe(lambda df: pd.merge_asof(
      df.sort_values("sorting_date"),
      annualPortfolios.sort_values("sorting_date"),
      on="sorting_date",
      by="permno",
      direction="backward"
    ))
  )

  joblib.dump(portfolios, "../data/portfolios-step-2-ff3.joblib")

ff3FactorsReplicated = (portfolios
  .groupby(["portfolio_size", "portfolio_bm", "date"])
  .apply(lambda x: pd.Series({
    "ret": np.average(x["ret_excess"], weights=x["mktcap_lag"])
    })
   )
  .reset_index()
  .groupby("date")
  .apply(lambda x: pd.Series({
    "smb": (
      x["ret"][x["portfolio_size"] == 1].mean() - 
        x["ret"][x["portfolio_size"] == 2].mean()),
    "hml": (
      x["ret"][x["portfolio_bm"] == 3].mean() -
        x["ret"][x["portfolio_bm"] == 1].mean())
    }))
  .reset_index()
)

# Saves ff3 factors replicated to a csv file
ff3FactorsReplicated.to_csv("factors-replicated-step-2.csv", index=False)
joblib.dump(ff3FactorsReplicated, '../data/ff3_factors_replicated_step_2.joblib')