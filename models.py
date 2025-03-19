from sqlalchemy import Column, String, Integer, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class CRSPMonthly(Base):
    __tablename__ = 'crsp_monthly'
    permno = Column(Integer, primary_key=True)
    gvkey = Column(String)
    date = Column(DateTime, primary_key=True)
    ret_excess = Column(Float)
    mktcap = Column(Float)
    mktcap_lag = Column(Float)
    exchange = Column(String)

class Compustat(Base):
    __tablename__ = 'compustat'
    gvkey = Column(String, primary_key=True)
    datadate = Column(DateTime, primary_key=True)
    be = Column(Float)  
    op = Column(Float)  
    inv = Column(Float) 
    filedate = Column(DateTime)

class FactorsFF3Monthly(Base):
    __tablename__ = 'factors_ff3_monthly'
    date = Column(DateTime, primary_key=True)
    mkt_excess = Column(Float)
    smb = Column(Float)  
    hml = Column(Float)
    rf = Column(Float)

class FactorsFF5Monthly(Base):
    __tablename__ = 'factors_ff5_monthly'
    date = Column(DateTime, primary_key=True)
    mkt_excess = Column(Float)
    smb = Column(Float)  
    hml = Column(Float)
    rmw = Column(Float)
    cma = Column(Float)
    rf = Column(Float)