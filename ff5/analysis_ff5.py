import sys
import os
import pandas as pd
import statsmodels.formula.api as smf
from regtabletotext import prettify_result

sys.path.append(os.path.abspath(".."))
from database_connector import DatabaseConnector

dc = DatabaseConnector(f"sqlite:///{os.path.abspath('../data/fama_french_data.sqlite')}")

# Analyze Flags
analyzeStep1: bool = True
analyzeStep2: bool = False
analyzeStep3: bool = False

""" FF5 Step 1 """
if analyzeStep1:
    ff5Replicated = pd.read_csv("factors-replicated-ff5-step-1.csv")
    ff5Replicated = ff5Replicated.rename(columns={'smb': 'smb_replicated', 'hml': 'hml_replicated', 'cma': 'cma_replicated', 'rmw': 'rmw_replicated'})

    ff5Replicated["date"] = pd.to_datetime(ff5Replicated["date"])
    dc.ff5Monthly["date"] = pd.to_datetime(dc.ff5Monthly["date"])

    ff5Replicated = (ff5Replicated
    .merge(dc.ff5Monthly, how="inner", on="date")
    .round(4)
    )

    modelSMB = (smf.ols(
        formula="smb ~ smb_replicated", 
        data=ff5Replicated
    )
    .fit()
    )
    prettify_result(modelSMB)

    modelHML = (smf.ols(
        formula="hml ~ hml_replicated", 
        data=ff5Replicated
    )
    .fit()
    )
    prettify_result(modelHML)

    modelCMA = (smf.ols(
        formula="cma ~ cma_replicated", 
        data=ff5Replicated
    )
    .fit()
    )
    prettify_result(modelCMA)

    modelRMW = (smf.ols(
        formula="rmw ~ rmw_replicated", 
        data=ff5Replicated
    )
    .fit()
    )
    prettify_result(modelRMW)