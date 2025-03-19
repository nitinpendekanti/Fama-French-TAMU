import sys
import os
import pandas as pd
import statsmodels.formula.api as smf
import joblib
from regtabletotext import prettify_result

sys.path.append(os.path.abspath(".."))
from database_connector import DatabaseConnector

# Analyze Flags
analyzeStep1: bool = True
analyzeStep2: bool = False
analyzeStep3: bool = False

# Use Cache
useCache: bool = False

if not useCache:    
    dc = DatabaseConnector(f"sqlite:///{os.path.abspath('../data/fama_french_data.sqlite')}")

def modelData(ff3Replicated) -> None:
    start_date = pd.to_datetime("01/01/1960", format="%m/%d/%Y")
    end_date = pd.to_datetime("12/31/2023", format="%m/%d/%Y")

    ff3Replicated = ff3Replicated.rename(columns={'smb': 'smb_replicated', 'hml': 'hml_replicated'})

    if useCache:
        ff3Monthly = joblib.load('../data/ff3Monthly.joblib')
    else:
        ff3Monthly = dc.ff3Monthly
        joblib.dump(ff3Monthly, '../data/ff3Monthly.joblib')
    
    ff3Replicated = ff3Replicated[(ff3Replicated['date'] >= start_date) & (ff3Replicated['date'] <= end_date)]
    ff3Monthly = ff3Monthly[(ff3Monthly['date'] >= start_date) & (ff3Monthly['date'] <= end_date)]

    ff3Replicated = (ff3Replicated
        .merge(ff3Monthly, how="inner", on="date")
        .round(4)
    )

    modelSMB = (smf.ols(
        formula="smb ~ smb_replicated", 
        data=ff3Replicated
    )
    .fit()
    )
    prettify_result(modelSMB)

    modelHML = (smf.ols(
        formula="hml ~ hml_replicated", 
        data=ff3Replicated
    )
    .fit()
    )
    prettify_result(modelHML)

""" FF3 Step 1 """
if analyzeStep1:
    ff3FactorsReplicated = joblib.load('../data/ff3_factors_replicated_step_1.joblib')
    modelData(ff3FactorsReplicated)