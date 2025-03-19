# Timely Factors Code Documentation

# Table of Contents

---

# Setup

## Clone the Project

## Python3 and Pip3

This project is setup via Python version 3.13.1. Ensure this version is on your computer by running `python3 —version`. Additionally, ensure pip3 is at its latest version by running `pip3 install —upgrade pip`. 

## Virtual Environment Setup

Ensure a virtual environment exists in the root directory labeled */env* or */venv*. To create a new virtual environment in the root directory run the following commands to generate and start the virtual environment:

`% python3 -m venv env`

`% source env/bin/activate` 

Before running the commands ensure you are in the root directory of the project:

`% cd path/to/directory` 

To print current working directory:

`% pwd`

## Dependencies

This project is using a requirements.txt file for dependency management. In order to install the dependencies run the following command:

`% pip3 install -r requirements.txt` 

If new dependencies are installed during future development, use the following command to update the requirements.txt file:

`$ pip3 freeze > requirements.txt`

---

# Top Down View of File Structure

![Untitled Diagram.drawio-5.png](Timely%20Factors%20Code%20Documentation%201b11264999d781fd9e5df790a00f6850/Untitled_Diagram.drawio-5.png)

## File Structure

- Fama-French-TAMU (root directory)
    - ff3
        - step_1_ff3.py
        - step_2_ff3.py
        - step_3_ff3.py
        - step_4_ff3.py
    - ff5
        - step_1_ff5.py
        - step_2_ff5.py
        - step_3_ff5.py
        - step_4_ff5.py
    - database_connector.py
    - models.py
    - .env
    - fetch_financial_data.py
    - requirements.txt

## fetch_financial_data.py

*THIS SQLITE FILE MAY NOT BE COMPLETELY ACCURATE PLEASE DOUBLE CHECK!*

*TIDY FINANCE DOES NOT INCLUDE MERGING 10-K FILING DATE INTO COMPUSTAT, THIS WAS DONE MANUALLY*

This file is mainly a sandbox for creating a local sqlite file. This sqlite file stores four tables of Compustat, CRSP, and Kenneth French’s FF3 & FF5 data from the data library. This sandbox allows us to locally store these tables and use them for future use. For more information on the results of this file go to [Local Database](https://www.notion.so/Timely-Factors-Code-Documentation-1b11264999d781fd9e5df790a00f6850?pvs=21). The documentation will not be covering the creation of the tables nor this file as it is not the focus however a future goal is to create a scalable & maintainable system of updating the local database to accommodate for new information. 

## models.py

This file creates models for the four tables that allows easy querying by the database connector. Fama French factors are calculated through the use of these tables. For how the models are constructed visit [Local Database](https://www.notion.so/Timely-Factors-Code-Documentation-1b11264999d781fd9e5df790a00f6850?pvs=21).

## database_connector.py

This file connects to the local sqlite file in /data and stores crsp_monthly, compustat, ff3, and ff5 objects in a class named `DatabaseConnector`. This allows us to access the data in the tables as a Python structure (from pandas library) called a Data frame. In our construction of the factors, we can easily construct them through these Data frame objects. Visit the [Database Constructor](https://www.notion.so/Timely-Factors-Code-Documentation-1b11264999d781fd9e5df790a00f6850?pvs=21) section for more information. 

## step_X_FF3.py / step_X_FF5.py

These files do the heavy lifting of the project.

## analysis.py

This file performs analysis and generates deliverables of the end results. 

---

# Local Database (models.py)

This project preemptively queried information from WRDS, Compustat, and Kenneth French’s data library (for Fama-french 3 and 5 data). The data is stored in a local sqlite3 file in the */data* directory and is currently about half a GB. There are four tables in the database titled crsp_monthly, compustat, factors_ff3_monthly, and factors_ff5_monthly. The rest of this section will discuss the columns of each of these tables and how the model is represented in the [`models.py`](http://models.py) file. The classes in `models.py` allows us to query the sqlite file easily which will be discussed later on. 

## Monthly Fama-French Three-Factor Model

The **Fama-French Three-Factor Model (FF3)** is an extension of the Capital Asset Pricing Model (CAPM), developed by Eugene Fama and Kenneth French. It explains stock returns based on three factors: market excess, SMB (small minus big), and HML (high minus low).

```python
class FactorsFF3Monthly(Base):
    __tablename__ = 'factors_ff3_monthly'
    date = Column(DateTime, primary_key=True)
    mkt_excess = Column(Float)
    smb = Column(Float)  
    hml = Column(Float)
    rf = Column(Float)
```

- **DATE**: ranges from 1960 to 2024
- **MKT_EXCESS**: $R_m-R_f$ ( $R_m$ is return to market)
- **SMB**: $R_{small}-R_{big}$ (based on market equity)
- **HML**: $R_{high} - R_{low}$ (based on book-to-market ratio)
- $R_f$: risk free return

## Monthly Fama-French Five-Factor Model

The **Fama-French Five-Factor Model (FF5)** is an extension of the **Three-Factor Model**, adding two more factors to improve the explanation of stock returns. 

```python
class FactorsFF5Monthly(Base):
    __tablename__ = 'factors_ff5_monthly'
    date = Column(DateTime, primary_key=True)
    mkt_excess = Column(Float)
    smb = Column(Float)  
    hml = Column(Float)
    rmw = Column(Float)
    cmw = Column(Float)
    rf = Column(Float)
```

- **DATE**: ranges from 1960 to 2024
- **MKT_EXCESS**: $R_m-R_f$ ( $R_m$ is return to market)
- **SMB**: $R_{small}-R_{big}$ (based on market equity)
- **HML**: $R_{high} - R_{low}$ (based on book-to-market ratio)
- **RMW**: $R_{robust} - R_{weak}$ (high profitability/robust tend to outperform low profitability/weak earnings)
- **CMA: $R_{conservative} - R_{aggressive}$ (**firms that invest conservatively (low asset growth) tend to outperform those that invest aggressively (high asset growth))
- $R_f$: risk free return

## CRSP Monthly Data

The CRSPMonthly class is a representation of a monthly record from the CRSP (Center for Research in Security Prices) dataset.

```python
class CRSPMonthly(Base):
    __tablename__ = 'crsp_monthly'
    permno = Column(Integer, primary_key=True)
    gvkey = Column(String)
    date = Column(DateTime, primary_key=True)
    ret_excess = Column(Float)
    mktcap = Column(Float)
    mktcap_lag = Column(Float)
    exchange = Column(String)
```

- **PERMNO:** Unique identifier given to each security in the CRSP database
- **GVKEY:** Unique identifier used in the Compustat database
- **DATE:** Date of data
- **RET_EXCESS:** Excess return of a security (return over an interval of time - risk-free rate of period)
- **MKTCAP:** Market capitalization of a stock during a given month (m)
- **MKTCAP_LAG:** Market capitalization of previous month (m - 1)
- **EXCHANGE:** Stock exchange security is traded (ie. NYSD, NASDAQ)

## Compustat Data

The Compustat class represents a table that stores fundamental accounting and financial data about firms.

```python
class Compustat(Base):
    __tablename__ = 'compustat'
    gvkey = Column(String, primary_key=True)
    datadate = Column(DateTime, primary_key=True)
    be = Column(Float)  
    op = Column(Float)  
    inv = Column(Float) 
    filedate = Column(DateTime)
```

- **GVKEY:** Global company key to uniquely identify firms in Compustat
- **DATADATE:** Date of the financial statement
- **BE: Book equity (used to calculate book-to-market)**
    - $Book Equity = Total Assets - Total Liabilities + Deferred Taxes - Preferred Stock$
- **OP: Operating Profitability**
    - $OperatingProfitability = \frac{Operating Income}{Book Equity}$
- **INV: Investment in assets**
    - $Investment = \frac{TotalAssets_t - TotalAssets_{t-1}}{TotalAssets_{t-1}}$
- **FILEDATE:** 10-k File date (from co_filedate table)
    - [https://wrds-www.wharton.upenn.edu/dynamic-query-form/comp_na_annual_all/co_filedate/](https://wrds-www.wharton.upenn.edu/dynamic-query-form/comp_na_annual_all/co_filedate/)
    

---

# Database Connector (database_connector.py)

## Libraries

### SQLAlchemy

SQLAlchemy is how we communicate with the database. Rather than utilizing raw SQL queries, we can utilize the models created described in the previous section. The start of any SQLAlchemy connection is using the `engine` object which can be thought of the central source of connections to the database by managing the connection pool. It can be created using these lines:

`from sqlalchemy import create_engine`

`engine = create_engine("sqlite:///data/name_of_db.sqlite")`

The connection string itself (string within the create_engine function) is the path to the sqlite file. 

The second part of SQLAlchemy to discuss are sessions. Sessions are an interface to executing queries. To setup a session look at the following lines:

`from sqlalchemy.orm import sessionmaker` 

`session = sessionmaker(bind=self.engine)`

`self.session = Session()`

Sessions allow you to query a table from the datbase using a mode.

```python
self.session.query(
	FactorsFF3Monthly.date,
	FactorsFF3Monthly.smb,
	FactorsFF3Monthly.hml
).all()
```

The following code retrieves a list of tuples from the database. The first few lines of output of the query are following:

<aside>

(datetime.datetime(1960, 1, 1, 0, 0), 0.0209, 0.0278),

(datetime.datetime(1960, 2, 1, 0, 0), 0.0051, -0.019299999999999998), 

(datetime.datetime(1960, 3, 1, 0, 0), -0.0049, -0.0294)

…

</aside>

### Pandas

The entire query from above is saved into a `DataFrame` object as so:

```python
self.ff3_monthly = pd.DataFrame(
	self.session.query(
		FactorsFF3Monthly.date,
		FactorsFF3Monthly.smb,
		FactorsFF3Monthly.hml
		).all(),
	columns=["date", "smb", "hml"]
).dropna()
```

This query returns a two-dimensional object that can be thought of as Python’s implementation of Excel, except it is controlled programmatically. The columns parameter sets the column names of the data returned from the query. Printing out the DataFrame shows an output like this:

<aside>

            date          smb       hml

0   1960-01-01  0.0209  0.0278
1   1960-02-01  0.0051 -0.0193
2   1960-03-01 -0.0049 -0.0294
3   1960-04-01  0.0032 -0.0228
4   1960-05-01  0.0121 -0.0370
..         ...     ...     ...
775 2024-08-01 -0.0355 -0.0113
776 2024-09-01 -0.0017 -0.0259
777 2024-10-01 -0.0101  0.0089
778 2024-11-01  0.0463 -0.0005
779 2024-12-01 -0.0273 -0.0295

[780 rows x 3 columns]

</aside>

Despite seeming just like an Excel file, pandas DataFrames have many neat abilities to easily manipulate the data. For example, the `dropna()` function drops any rows with an NA as one of the elements. There are many other abilities of Pandas DataFrames which will be discussed in more detail throughout the document. 

## DataConnector Class

This subsection will discuss the DataConnector class which converts sqlite tables to pandas DataFrames. There are three main functions within this class: __init__(), connect(), close(). Let’s begin with connect() and close():

```python
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
```

As discussed in the [SQLAlchemy](https://www.notion.so/Timely-Factors-Code-Documentation-1b11264999d781fd9e5df790a00f6850?pvs=21) subsection, the `create_engine()`  sets up the connection pool manager while `sessionmaker()` starts a new connection to the database. The code in connect is wrapped in a try & except format catch errors. The `close()` function simply closes the connection to the database which cleans up any file descriptors or buffers.

The __init__() function looks daunting, however is quite simple. The first few lines simply take in the connection_string parameter (path to sqlite file) and set up the engine & session by calling the `connect()` function described above. 

Four pandas DataFrame objects are created called crsp_monthly, compustat, ff3_monthly, and ff5_monthly. For more information on how the pandas dataframes and sessions work visit the [SQLAlchemy](https://www.notion.so/Timely-Factors-Code-Documentation-1b11264999d781fd9e5df790a00f6850?pvs=21) and [Pandas](https://www.notion.so/Timely-Factors-Code-Documentation-1b11264999d781fd9e5df790a00f6850?pvs=21) section.

```python
def __init__(self, connection_string: str):
    self.connection_string = connection_string
    self.engine = None
    self.session = None
    self.connect()

    self.crsp_monthly = pd.DataFrame(
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

    self.ff3_monthly = pd.DataFrame(
        self.session.query(
            FactorsFF3Monthly.date,
            FactorsFF3Monthly.smb,
            FactorsFF3Monthly.hml
        ).all(),
        columns=["date", "smb", "hml"]
    ).dropna()

    self.ff5_monthly = pd.DataFrame(
        self.session.query(
            FactorsFF5Monthly.date,
            FactorsFF5Monthly.smb,
            FactorsFF5Monthly.hml,
            FactorsFF5Monthly.rmw,
            FactorsFF5Monthly.cmw
        ).all(),
        columns=["date", "smb", "hml", "rmw", "cmw"]
    ).dropna()
```

---

# Timely Factors Creation (step_X_FF(5/3).py)

## Background

The meat of this project is within these files. For clarity there are two folders of the code: one for replication of Fama-French 3 factors and one for Fama-French 5 factors. Within each of them there are multiple versions of the code. The versions are described below:

- Version 1: Replication of Fama French 3 and 5 without considering timing (goal is to replicate Kenneth French’s data as close as possible)
    - This code is written by using [Tidy-Finance](https://www.tidy-finance.org/python/replicating-fama-and-french-factors.html)
- Version 2: Rather than updating market equity every December or July, it is updated monthly
- Version 3: Book equity is updated every time a new 10-K is filed for a firm
- Version 4: Calculate daily returns in addition to monthly factor returns

As a reminder revisit the [directory structure.](https://www.notion.so/Timely-Factors-Code-Documentation-1b11264999d781fd9e5df790a00f6850?pvs=21) Due to having multiple files where there are only some differences file to file, there is redundancy. The rest of this section will discuss the code starting with the Fama-French 3 implementation and moving on to the Fama-French 5. 

## Cache

There are two levels of caching in this system. The first one is caching tables from WRDS, Compustat, and Kenneth French’s data library in a local sqlite file. Sqlite is simply a small, local SQL database engine and for this project it is located in the `/data` directory. The next caches are in `.joblib` files. The .joblib files are stored in the same directory and are used to cache intermediary tables such as `marketEquityHML` and `marketEquitySMB` . This is able to speed up the system by a large amount. At the top of each of the files, there is a flag called `useCache: bool` . When set to true, it will use the locally stored .joblib files. When set to false, it will query the sqlite database and cache that information in the .joblib files. Note that this does it for every single query, to individually configure the caching, you must go to each if else block and set the flag to true or false. 

To cache/save a table via job lib (`marketEquitySMB` is a pandas DataFrame and `../data/marketEquitySMB.joblib` is the location saved):

`joblib.dump(marketEquitySMB, "../data/marketEquitySMB.joblib")` 

To load a table from cache we use this command:

`marketEquitySMB = joblib.load("../data/marketEquitySMB.joblib")` .

The rest of this document will assume that caching is turned off, however in production environment this flag is turned on. 

## Fama French 3

### Step 1: Replication of FF3

**Step 1.1: Data Preparation**

At the start of all these files we create a local object connecting to the local sqlite3 database using this line:

`dc = DatabaseConnector(f"sqlite:///{os.path.abspath('../data/fama_french_data.sqlite')}")`

The next step is querying the crspMonthly file to get the market equity for each firm:

```python
"""Get Market Equity for SMB"""
marketEquitySMB = (dc.crspMonthly
  .query("date.dt.month == 6")
  .assign(sorting_date=lambda x: (x["date"]+pd.DateOffset(months=1)))
  .get(["permno", "exchange", "sorting_date", "mktcap"])
  .rename(columns={"mktcap": "size"})
)

"""Get Market Equity for HML"""
marketEquityHML = (dc.crspMonthly
  .query("date.dt.month == 12")
  .assign(sorting_date=lambda x: (x["date"]+pd.DateOffset(months=7)))
  .get(["permno", "gvkey", "sorting_date", "mktcap"])
  .rename(columns={"mktcap": "me"})
)
```

We currently have two DataFrames where each of them are from crspMonthly however one of them is used to calculate SMB and the other for HML. Both of the frames get the table from `dc.crspMonthly` which is discussed above. On the next line, marketEquitySMB and marketEquityHML query every row with a month of June and December respectively. The MOST IMPORTANT thing to note is what month of market equity is being used for SMB and HML calculation. 

For SMB calculation, from July of year ***t*** to June of year ***t + 1***, we are using June of year ***t’s*** market equity. For the HML calculation, from July of year t to June of year ***t + 1***, we are using December of year ***t-1’s*** market equity.  

 

For clarity lets look at SMB market equity:

```
         permno exchange  sorting_date   size
12        10024   NASDAQ   1986-07-01  16.676000
24        10024   NASDAQ   1987-07-01  12.950000
...
```

```
         permno   gvkey       date  ret_excess       size  mktcap_lag exchange
12        10024  004008 1986-06-01    0.274800  16.676000   13.028125   NASDAQ
24        10024  004008 1987-06-01   -0.151141  12.950000   15.170000   NASDAQ
```

HML market equity table:

```
         permno  exchange sorting_date     me
18        10024   NASDAQ   1987-07-01  15.358250
30        10024   NASDAQ   1988-07-01   7.797562
...
```

```
         permno   gvkey    date     ret_excess      me      mktcap_lag exchange
18        10024  004008 1986-12-01   -0.153048  15.358250   18.029250   NASDAQ
30        10024  004008 1987-12-01   -0.090857   7.797562    8.540188   NASDAQ
```

Now lets take a look at the book-to-market calculation for the HML factor:

```python
bookToMarket = (dc.compustat
  .assign(
    sorting_date=lambda x: (pd.to_datetime(
      (x["datadate"].dt.year+1).astype(str)+"0701", format="%Y%m%d")
    )
  )
  .merge(marketEquityHML, how="inner", on=["gvkey", "sorting_date"])
  .assign(bm=lambda x: x["be"]/x["me"])
  .get(["permno", "sorting_date", "me", "bm"])
)
```

This code will iterate through every row in the compustat table and for every book equity found, it will create a sorting date variable which is July 1st of year ***t+1***. Then it merges with the `marketEquityHML` table above to extract the market equity associated with July to June. For example, let's take a look at a company with a permno of 10015, data date of 1984-12-31, and a book equity of 9.319. The sorting date is set to 1985-07-01 because the year of data date is incremented by one and the day and month is set to July 1st. Now there is an inner join on the gvkey and sorting date where we can get the information associated with a gvkey of 10015 and a sorting date of 1985-07-01 from the marketEquityHML table. The market equity associated with that row is 13.61. If we divide 9.319 by 13.61 we get a book-to-market of 0.68. This is how the calculation is done for every date. 

Now we have bookToMarket and marketEquitySMB rows all with a sorting date of July 1st, therefore we are able to merge both the book-to-market and market equity on the sorting date and permno as so:

```python
sortingVariables = (marketEquitySMB
  .merge(bookToMarket, how="inner", on=["permno", "sorting_date"])
  .dropna()
  .drop_duplicates(subset=["permno", "sorting_date"])
 )
```

**Step 1.2: Portfolio Construction**

```python
portfolios = (sortingVariables
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
```

The groupby function on the second line groups rows in the sortingVariables table by their sorting date. 

- Groups sortingVariables by sorting date
- Each group is a sub-DataFrame
- Each group contains all stocks with the same sorting date
- Each group contains multiple stocks from multiple exchanges
- The assign() method assigns columns for every sub-DataFrame/group
- Within each group, assign_portfolio() assigns portfolio_size and portfolio_bm to each stock

To better understand how the binning is working let’s take a look at assign_portfolios():

```python
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
```

The function takes three parameters: data, sorting_variable, percentiles:

- `data` is the sub-DataFrame or group that is a portfolio of stocks with the same sorting_date
- sorting_variable is whether the data will be put into bins based on size/market equity or book-to-market
- The percentiles show how to split up the data (sorting_variable) into bins.
    - For SMB [0, 0.5, 1] is used where 0.5 to 1 is big and 0 to 0.5 is small
    - For HML [0, 0.3, 0.7, 1] is used where 0.7-1 is high, 0.3-0.7 is medium and 0-0.3 is low

Now let's take a look at the breakpoints assignment

- We query the NYSE exchange as Fama-French only uses data from that exchange
- We extract the sorting variable which is either size/market equity (SMB) or book-to-market (HML)

<aside>

Breakpoint Output for One Portfolio:

0.0        -inf
0.5    79.73925
1.0         inf
Name: size, dtype: float64

0.0        -inf
0.3    0.289517
0.7    1.305713
1.0         inf
Name: bm, dtype: float64

</aside>

- In the top, if a value is between -inf (0th percentile) and 79.74 (50th percentile) it is set to small and if it is above the 50th percentile it is set to big
- Similarly for book-to-market, between the 0th and 30th percentile lies low, for 30th and 70th percentile lies medium, and 70th to 100th percent is high

Now for the last portion of the code where we set assigned_portfolios it simply puts each stock within a group inside a bin based on the breakpoints

- Binning
    - For SMB
        - Assigns 1 if between 0th and 50th percentile
        - Assigns 2 if between 50th and 100th percentile
    - For HML
        - Assigns 1 if between 0th and 30th percentile
        - Assigns 2 if between 30th and 70th percentile
        - Assigns 3 if between 70th and 100th percentile
- `right=False` simply makes the binning left inclusive
    - [a, b) instead of (a, b]
    - This means that 79.73925 is within bin 2 for SMB (for this specific example)
- `labels=pd.Series(range(1, breakpoints.size))` assigns portfolio labels (1, 2, 3, ...)

```python
portfolios = (dc.crspMonthly
  .assign(
    sorting_date=lambda x: (pd.to_datetime(
      x["date"].apply(lambda x: str(x.year-1)+
        "0701" if x.month <= 6 else str(x.year)+"0701")))
  )
  .merge(portfolios, how="inner", on=["permno", "sorting_date"])
)
```

This code merges the crspMonthly data with the portfolios. The main thing to know is as so:

- If the date of entry is between January and June inclusive, set the date to July 1st of year ***t-1***
- If the date of entry is between July and December inclusive, set the date to July 1st of year ***t***

**Step 1.3: Fama-French 3 Factor Calculation**

```python
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
```

First let's notice that there are three possibilities for HML and two possibilities for SMB. This gives us 6 possible bins:

- Small/Low
- Small/Medium
- Small/High
- Big/Low
- Big/Medium
- Big/High

The groupby() function in this code block, groups each stock so that each group has the same date, size value (small or big), and book-to-market value (low, medium, or high). For the apply() function here is what happens:

- `x["ret_excess"]` is the column containing excess returns for the stocks in that group.
- `x["mktcap_lag"]` contains the market capitalization of each stock **from the previous period** (used as weights).
- `np.average(x["ret_excess"], weights=x["mktcap_lag"])` computes a **weighted average** of excess returns, where:
    - Stocks with **higher market capitalization** have more influence on the final return.
- The formula for a weighted average is:

$$
\text{Weighted Average} = \frac{\sum ( \text{ret\_excess}_i \times \text{mktcap\_lag}_i )}{\sum \text{mktcap\_lag}_i}
$$

For the next section, it simply calculates smb by taking the mean return of the small stocks (1) minus the mean return of the large stocks (2). Similarly, HML takes the mean of the high stocks (3) minus the mean of the low stocks (1). `reset_index()` is called after both `group_by()`s to put the columns back to the way it was. 

### Step 2: Update ME Monthly

The first change is done to remove the filtering of only June for the marketEquitySMB by removing this line `.query("date.dt.month == 6")`:

```python
marketEquitySMB = (dc.crspMonthly
    .assign(sorting_date=lambda x: (x["date"]+pd.DateOffset(months=1)))
    .get(["permno", "exchange", "sorting_date", "mktcap"])
    .rename(columns={"mktcap": "size"})
 )
```

The second change also removes the month filtering of December from the marketEquityHML but also modifies the date offset to the next month:

```python
marketEquityHML = (dc.crspMonthly
    .assign(sorting_date=lambda x: (x["date"]+pd.DateOffset(months=1)))
    .get(["permno", "gvkey", "sorting_date", "mktcap"])
    .rename(columns={"mktcap": "me"})
 )
```

Rather than creating annual portfolios based on July or December market equities, we now consider every month. The next step we must do is update how we create the portfolios. First go to the [previous method](https://www.notion.so/Timely-Factors-Code-Documentation-1b11264999d781fd9e5df790a00f6850?pvs=21) for dealing with portfolio dates.

```python
portfolios = (dc.crspMonthly
    .assign(
      sorting_date=lambda x: x["date"] + pd.DateOffset(months=1) - 
                             pd.DateOffset(days=x["date"].dt.day-1)
    )

    .pipe(lambda df: pd.merge_asof(
      df.sort_values("sorting_date"),
      annualPortfolios.sort_values("sorting_date"),
      on="sorting_date",
      by="permno",
      direction="backward"
    ))
  )
```

- **`pd.DateOffset(days=x["date"].dt.day-1)`**: This subtracts the days of the month, effectively setting the date to the first day of the month. For example, if `x["date"]` is `2023-05-10`, `x["date"].dt.day` is `10`, so `pd.DateOffset(days=10-1)`will subtract 9 days, resulting in `2023-05-01`.
    - This effectively creates a sorting date that is the first of every month
- Pipe is a way to perform functions on a pandas DataFrame
- **`df.sort_values("sorting_date")`**: Sorts the `crspMonthly` DataFrame by `sorting_date`
- **`pd.merge_asof()`**This is where the magic happens. This is a type of merge used for time series or ordered data, and it matches rows based on the nearest key rather than requiring an exact match. Here are the parameters:
    - **`on="sorting_date"`**: Specifies that the merge should be based on the `sorting_date` column.
    - **`by="permno"`**: This tells `merge_asof` to perform the merge on `permno` (
    - **`direction="backward"`**: This is the key part. It tells `merge_asof` to look **backwards** in the sorted `annualPortfolios` DataFrame and find the most recent `sorting_date` that is less than or equal to the `sorting_date` in `df`. This ensures that each month gets the most recent portfolio classification from the annual portfolios.

### Step 3: Updating BE Dynamically

```php
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
```

- Rather than creating a sorting date on July for book equity, we know assign it to month ***m+1*** where month ***m*** is when the 10-k file date occurs
- `sorting_date=lambda x: (x["filedate"] + pd.DateOffset(months=1) - pd.DateOffset(days=x["filedate"].dt.day-1))` sets the sorting date to the first day of month ***m+1***
    - ie. if 10-k filing date was 07-20-1990, the sorting date becomes 08-01-1990

## Fama French 5

Due to duplicity between Fama French 3 and Fama French 5 code, only the sections that very will be discussed in this part.

### Step 1: Replication of FF5

**Step 1.1: Data Preparation** 

```python
sortingVarsExtended = (dc.compustat
.assign(
    sorting_date=lambda x: (pd.to_datetime(
    (x["datadate"].dt.year+1).astype(str)+"0701", format="%Y%m%d")
    )
)
.merge(marketEquityHML, how="inner", on=["gvkey", "sorting_date"])
.assign(bm=lambda x: x["be"]/x["me"])
.get(["permno", "sorting_date", "me", "bm", "op", "inv"])
)

sorting_variables = (marketEquitySMB
  .merge(sortingVarsExtended, how="inner", on=["permno", "sorting_date"])
  .dropna()
  .drop_duplicates(subset=["permno", "sorting_date"])
)
```

The only difference for the FF5 data preperation is that we extend the compustat query to include operating profitability (op) and investment (inv) of the firm. Otherwise the section stays the same as the FF3 sorting variable table. 

Step 1.2: Portfolios Construction

```python
portfolios = (sortingVariables
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
```

1. Initially the DataFrame is grouped by the sorting date column
2. `portfolio_size=assign_portfolio(x, "size", [0, 0.5, 1])`  bins the stocks within a group into size (small or big) relative to the group.
3. The index is reset.
4. The DataFrame is now grouped by the sorting date column and the size column calculated in [step 2](https://www.notion.so/Timely-Factors-Code-Documentation-1b11264999d781fd9e5df790a00f6850?pvs=21).
5. Within each size group, additional sorting is performed for **bm, op, and inv** using:
    - `assign_portfolio(x, "bm", [0, 0.3, 0.7, 1])` (Low, Medium, High book-to-market)
    - `assign_portfolio(x, "op", [0, 0.3, 0.7, 1])` (Low, Medium, High profitability)
    - `assign_portfolio(x, "inv", [0, 0.3, 0.7, 1])` (Low, Medium, High investment)
6. Final cleanup is done with the reset and querying the necessary columns. 

After this, the sorting date’s for the portfolio are adjusted like the [FF3 section.](https://www.notion.so/Timely-Factors-Code-Documentation-1b11264999d781fd9e5df790a00f6850?pvs=21)

**Step 1.3: Factors Construction**

> Now, we want to construct each of the factors, but this time, the size factor actually comes last because it is the result of averaging across all other factor portfolios. This dependency is the reason why we keep the table with value-weighted portfolio returns as a separate object that we reuse later. - TidyFinance
> 

```python
portfoliosValue = (portfolios
  .groupby(["portfolio_size", "portfolio_bm", "date"])
  .apply(lambda x: pd.Series({
      "ret": np.average(x["ret_excess"], weights=x["mktcap_lag"])
    })
  )
  .reset_index()
)

factors_value = (portfoliosValue
  .groupby("date")
  .apply(lambda x: pd.Series({
    "hml": (
      x["ret"][x["portfolio_bm"] == 3].mean() - 
        x["ret"][x["portfolio_bm"] == 1].mean())})
  )
  .reset_index()
)
```

Similar to the FF3 HML calculation, this code groups the portfolio by size (small or big), book-to-market (low, medium, or high) and date. The return is calculated again using a weighted average and the HML column is calculated by taking the average of the high stocks minus the average of the low stocks for each date. 

```python
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
```

Now moving onto the RMW factor, we group by size, operability profit, and date and calculate a weighted average resulting in a new DataFrame named `portfoliosProfitability`. The Robust Minus Weak column is calculated by taking the average of high profitability firms for a specific month and subtracting the low profitability firms of the same month. 

```python
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
```

Again, a new DataFrame is initialized by calculating the weighted average of groups grouped by size, investment, and date. After this, we calculate the average of conservative stock return (low investments) minus the average of aggressive stock returns (high investment) for each date. 

```python
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
```

Since size is a dependant variable for the preceding factors, it is calculated last. Similarly to FF3, it is simply small stocks minus big stocks per each month. The **`pd.concat`** function is used here to **combine multiple factor datasets (`portfoliosValue`, `portfoliosProfitability`, `portfoliosInvestment`) into a single DataFrame** before computing the **Size Minus Big (SMB) factor**. In the Fama-French methodology, size factors for FF5 are computed accross multiple dimensions. 

```python
ff5FactorsReplicated = (factorsSize
  .merge(factors_value, how="outer", on="date")
  .merge(factors_profitability, how="outer", on="date")
  .merge(factors_investment, how="outer", on="date")
)
```

Finally, all four tables with the four factors are put into one table

### Step 2: Updating ME Monthly

Step two of the Fama-French five code includes the same changes made as the Fama-french three code. 

### Step 3: Updating BE After Each 10-K Filing Date

```php
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
```

- Same as FF3 change except applying the code change on sortingVarsExtended