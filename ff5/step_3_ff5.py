""" Fama French 5 Factor Replication with Dynamic Book Equity Updates """

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

"""Get Market Equity for SMB"""
if useCache:
    marketEquitySMB = joblib.load("../data/marketEquitySMBFF5_updated.joblib")
else:
  marketEquitySMB = (dc.crspMonthly
    .assign(sorting_date=lambda x: (x["date"]+pd.DateOffset(months=1)))
    .get(["permno", "exchange", "sorting_date", "mktcap"])
    .rename(columns={"mktcap": "size"})
  )
  joblib.dump(marketEquitySMB, "../data/marketEquitySMBFF5_updated.joblib")

"""Get Market Equity for HML"""
if useCache:
  marketEquityHML = joblib.load("../data/marketEquityHMLFF5_updated.joblib")
else:
  marketEquityHML = (dc.crspMonthly
    .assign(sorting_date=lambda x: (x["date"]+pd.DateOffset(months=1)))
    .get(["permno", "gvkey", "sorting_date", "mktcap"])
    .rename(columns={"mktcap": "me"})
  )
  joblib.dump(marketEquityHML, "../data/marketEquityHMLFF5_updated.joblib")

""" Get Sorting Variables: Me, Op, Inv and calculate Bm with filing dates"""
if useCache:
  sortingVarsExtended = joblib.load("../data/sortingVarsExtendedFF5_updated.joblib")
else:
  # Get compustat data with filedate and datadate and all needed variables
  sortingVarsExtended = (dc.compustat
    # Use the filedate (+1 month) as the sorting date when BE gets updated
    .assign(
      sorting_date=lambda x: (x["filedate"] + pd.DateOffset(months=1) - pd.DateOffset(days=x["filedate"].dt.day-1))
    )
    .get(["permno", "gvkey", "datadate", "filedate", "sorting_date", "be", "op", "inv"])
    # For each permno and sorting_date, merge with market equity
    .merge(marketEquityHML, how="inner", on=["gvkey", "sorting_date"])
    .assign(bm=lambda x: x["be"]/x["me"])
    .get(["permno", "gvkey", "datadate", "filedate", "sorting_date", "me", "be", "bm", "op", "inv"])
  )
  joblib.dump(sortingVarsExtended, "../data/sortingVarsExtendedFF5_updated.joblib")

# Merge market equity with accounting variables
sortingVariables = (marketEquitySMB
  .merge(sortingVarsExtended, how="inner", on=["permno", "sorting_date"])
  .dropna()
  .drop_duplicates(subset=["permno", "sorting_date"])
)

# Create monthly portfolios (instead of annual)
monthlyPortfolios = (sortingVariables
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

if useCache:
  portfolios = joblib.load("../data/portfolios-ff5_updated.joblib")
else:
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
   
  joblib.dump(portfolios, "../data/portfolios-ff5_updated.joblib")

""" HML Table Calculation """
portfoliosValue = (portfolios
  .groupby(["portfolio_size", "portfolio_bm", "date"])
  .apply(lambda x: pd.Series({
      "ret": np.average(x["ret_excess"], weights=x["mktcap_lag"])
    })
  )
  .reset_index()
)

factorsValue = (portfoliosValue
  .groupby("date")
  .apply(lambda x: pd.Series({
    "hml": (
      x["ret"][x["portfolio_bm"] == 3].mean() - 
        x["ret"][x["portfolio_bm"] == 1].mean())})
  )
  .reset_index()
)

""" RMW Table Calculation """
portfoliosProfitability = (portfolios
  .groupby(["portfolio_size", "portfolio_op", "date"])
  .apply(lambda x: pd.Series({
      "ret": np.average(x["ret_excess"], weights=x["mktcap_lag"])
    })
  )
  .reset_index()
)

factorsProfitability = (portfoliosProfitability
  .groupby("date")
  .apply(lambda x: pd.Series({
    "rmw": (
      x["ret"][x["portfolio_op"] == 3].mean() - 
        x["ret"][x["portfolio_op"] == 1].mean())})
  )
  .reset_index()
)

""" CMA Table Calculation """
portfoliosInvestment = (portfolios
  .groupby(["portfolio_size", "portfolio_inv", "date"])
  .apply(lambda x: pd.Series({
      "ret": np.average(x["ret_excess"], weights=x["mktcap_lag"])
    })
  )
  .reset_index()
)

factorsInvestment = (portfoliosInvestment
  .groupby("date")
  .apply(lambda x: pd.Series({
    "cma": (
      x["ret"][x["portfolio_inv"] == 1].mean() - 
        x["ret"][x["portfolio_inv"] == 3].mean())})
  )
  .reset_index()
)

""" SMB Table Calculation """
factorsSize = (
  pd.concat(
    [portfoliosValue, portfoliosProfitability, portfoliosInvestment], 
    ignore_index=True
  )
  .groupby("date")
  .apply(lambda x: pd.Series({
    "smb": (
      x["ret"][x["portfolio_size"] == 1].mean() - 
        x["ret"][x["portfolio_size"] == 2].mean())})
  )
  .reset_index()
)

""" Resultant Portfolio """
ff5FactorsReplicated = (factorsSize
  .merge(factorsValue, how="outer", on="date")
  .merge(factorsProfitability, how="outer", on="date")
  .merge(factorsInvestment, how="outer", on="date")
)

# Save to csv (comment out if not necessary)
ff5FactorsReplicated.to_csv("factors-replicated-ff5-updated.csv", index=False)
joblib.dump(ff5FactorsReplicated, '../data/ff5_factors_replicated_updated.joblib')