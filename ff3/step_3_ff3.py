""" Fama French 3 Step 3 Replication with Dynamic Book Equity Updates """

import sys
import os
import joblib
import numpy as np

sys.path.append(os.path.abspath(".."))
from database_connector import DatabaseConnector
import pandas as pd

# If set to true, use the cached queries, else it will requery the sqlite database using DatabaseConnector
useCache: bool = False  # Set to False to re-create data with the new approach

# If set to true, connects to sqlite database (must be set to True if useCache is False)
useDC: bool = True

# Create Connection to Database and Get Access to Tables (redo this when database changes)
if useDC:
  dc = DatabaseConnector(f"sqlite:///{os.path.abspath('../data/fama_french_data.sqlite')}")

"""Get Market Equity for SMB"""
if useCache:
    marketEquitySMB = joblib.load("../data/marketEquitySMBFF3Step3_updated.joblib")
else:
  marketEquitySMB = (dc.crspMonthly
    .assign(sorting_date=lambda x: (x["date"]+pd.DateOffset(months=1)))
    .get(["permno", "exchange", "sorting_date", "mktcap"])
    .rename(columns={"mktcap": "size"})
  )
  joblib.dump(marketEquitySMB, "../data/marketEquitySMBFF3Step3_updated.joblib")

"""Get Market Equity for HML"""
if useCache:
  marketEquityHML = joblib.load("../data/marketEquityHMLFF3Step3_updated.joblib")
else:
  marketEquityHML = (dc.crspMonthly
    .assign(sorting_date=lambda x: (x["date"]+pd.DateOffset(months=1)))
    .get(["permno", "gvkey", "sorting_date", "mktcap"])
    .rename(columns={"mktcap": "me"})
  )
  joblib.dump(marketEquityHML, "../data/marketEquityHMLFF3Step3_updated.joblib")

"""Get Book Equity with filing dates"""
if useCache:
  bookToMarket = joblib.load("../data/bookToMarketFF3Step3_updated.joblib")
else:
  # Get compustat data with filedate and datadate
  bookToMarket = (dc.compustat
    # Use the filedate (+1 month) as the sorting date when BE gets updated
    .assign(
      sorting_date=lambda x: (x["filedate"] + pd.DateOffset(months=1) - pd.DateOffset(days=x["filedate"].dt.day-1))
    )
    .get(["permno", "gvkey", "datadate", "filedate", "sorting_date", "be"])
    # For each permno and sorting_date, merge with market equity
    .merge(marketEquityHML, how="inner", on=["gvkey", "sorting_date"])
    .assign(bm=lambda x: x["be"]/x["me"])
    .get(["permno", "gvkey", "datadate", "filedate", "sorting_date", "me", "be", "bm"])
  )
  joblib.dump(bookToMarket, "../data/bookToMarketFF3Step3_updated.joblib")

"""Merge Market Equity (SMB) and Book-To-Market (HML) Into one table"""
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
  portfolios = joblib.load("../data/portfolios-step-3-ff3_updated.joblib")
else:
  # Now we create portfolios at each sorting date (monthly)
  monthlyPortfolios = (sorting_variables
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

  # Use merge_asof to match each stock with its most recent portfolio assignment
  portfolios = (dc.crspMonthly
    .assign(
      sorting_date=lambda x: x["date"].apply(
        lambda d: d + pd.DateOffset(months=1) - pd.DateOffset(days=d.day-1)
      )
    )
    .pipe(lambda df: pd.merge_asof(
      df.sort_values("sorting_date"),
      monthlyPortfolios.sort_values("sorting_date"),
      on="sorting_date",
      by="permno",
      direction="backward"
    ))
  )

  joblib.dump(portfolios, "../data/portfolios-step-3-ff3_updated.joblib")

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
ff3FactorsReplicated.to_csv("factors-replicated-step-3-updated.csv", index=False)
joblib.dump(ff3FactorsReplicated, '../data/ff3_factors_replicated_step_3_updated.joblib')