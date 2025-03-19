from models import CRSPMonthly, Compustat, FactorsFF3Monthly, FactorsFF5Monthly
import pandas as pd
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

class DatabaseConnector:
    """Handles database connections"""
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.engine = None
        self.session = None
        self.connect()

        self.crspMonthly = pd.DataFrame(
            self.session.query(
                CRSPMonthly.permno, CRSPMonthly.gvkey, CRSPMonthly.date,
                CRSPMonthly.ret_excess, CRSPMonthly.mktcap,
                CRSPMonthly.mktcap_lag, CRSPMonthly.exchange
            ).all(),
            columns=["permno", "gvkey", "date", "ret_excess", "mktcap", 
                    "mktcap_lag", "exchange"]
        ).dropna()

        self.compustat = pd.DataFrame(
            self.session.query(
                Compustat.gvkey, Compustat.datadate,
                Compustat.be, Compustat.op, Compustat.inv,
                Compustat.filedate
            ).all(),
            columns=["gvkey", "datadate", "be", "op", "inv", "filedate"] # book equity, operating profitability, investment, 10-k file date
        ).dropna()

        self.ff3Monthly = pd.DataFrame(
            self.session.query(
                FactorsFF3Monthly.date,
                FactorsFF3Monthly.smb,
                FactorsFF3Monthly.hml
            ).all(),
            columns=["date", "smb", "hml"]
        ).dropna()

        self.ff5Monthly = pd.DataFrame(
            self.session.query(
                FactorsFF5Monthly.date,
                FactorsFF5Monthly.smb,
                FactorsFF5Monthly.hml,
                FactorsFF5Monthly.rmw,
                FactorsFF5Monthly.cma
            ).all(),
            columns=["date", "smb", "hml", "rmw", "cma"]
        ).dropna()

    def connect(self):
        """Establishes local database connection"""
        try:
            self.engine = create_engine(self.connection_string)
            Session = sessionmaker(bind=self.engine)
            self.session = Session()
            print("Connection to Local DB established successfully.")
        except Exception as e:
            print(f"Error occurred while connecting to Local DB: {e}")
            raise

    def close(self):
        """Closes local database connection"""
        if self.session:
            self.session.close()
            print("Connection to Local DB Closed.")
    
