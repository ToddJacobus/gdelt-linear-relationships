# import pdb; pdb.set_trace()
import requests, zipfile, io, re, csv, json
from threading import Thread, Condition
from queue import Queue
import time, random, json

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import *
from sqlalchemy import Column, Integer, Float, String
from sqlalchemy.orm import sessionmaker

# -- UTILITY FUNCTIONS ---------------------------------------------------------
def getDbParams(params_path):
    with open(params_path, 'r') as file:
        params = json.load(file)
    return params

def createTable(tableMap, engine):
    if not engine.dialect.has_table(engine, tableMap.__tablename__):
        tableMap.__table__.create(engine)
    return tableMap.__table__

# -- SQLACHEMY TABLE MAPS ------------------------------------------------------
Base = declarative_base()
class Counts(Base):
    __tablename__ = "gdelt_v2_gkg_2018_counts"
    id = Column(Integer, primary_key=True)
    location = Column(String)
    theme = Column(String)
    count = Column(String)

class Analysis(Base):
    # TODO: finish this one...
    __tablename__ = 'gdelt_v2gdelt_v2_gkg_2018_analysis'
    id = Column(Integer, primary_key=True)
    query = Column(String)
    slope = Column(Float)
    intercept = Column(Float)
    r_value = Column(Float)
    p_value = Column(Float)
    std_err = Column(Float)
    date = Column(Integer)
    n = Column(Integer)

class Gdelt_v2(Base):
    __tablename__ = "gdelt_v2_gkg_2018"
    id = Column(Integer, primary_key=True)
    gkgrecordid = Column(String)
    date = Column(String)
    document_identifier = Column(String)
    v2_themes = Column(String)
    v2_locations = Column(String)
    v2_tone = Column(String)
    country_codes = Column(String)

# -- THREADING CLASSES ---------------------------------------------------------

class QueryThread(Thread):
    # consume a list of unique country codes and query the db with each one,
    # counting the number of records that have that country code.
    def __init__(self, country_queue, theme, sessionmaker):
        Thread.__init__(self)
        self.country_queue = country_queue
        self.theme = theme
        self.sessionmaker = sessionmaker
    def run(self):
        while True:
            country = self.country_queue.get()
            session = self.sessionmaker()
            try:
                count = session.query(Gdelt_v2).filter(Gdelt_v2.country_codes.like('%{}%'.format(country)), Gdelt_v2.v2_themes.like('%{}%'.format(self.theme))).count()
                # import pdb; pdb.set_trace()
                # Upload count to database...
                count_row = Counts(
                    location=country,
                    theme = self.theme,
                    count = count
                )
                session.add(count_row)
                session.commit()

            except:
                session.rollback()
                raise
            finally:
                session.close()

            self.country_queue.task_done()
            print(self.country_queue.qsize())
            if self.country_queue.qsize() < 1:
                return

            # count number of records with country code

# -- ANALYSIS FUNCTIONS --------------------------------------------------------

def analyze():
    # SELECT

    df = pandas.DataFrame.from_dict(count_results)

    # create normalized columns in df
    def normalize(mean, std, x):
        z = (x - mean)/std
        return z

    # import pdb; pdb.set_trace()

    df['gdp_z'] = df['gdp'].apply(lambda x: normalize(df['gdp'].mean(), df['gdp'].std(), x))
    df['count_z'] = df['count'].apply(lambda x: normalize(df['count'].mean(), df['count'].std(), x))

    if plot:
        df.plot(x='gdp_z', y='count_z', style='o')
        plt.title('Article Counts versus GDP: {}'.format(query))
        plt.xlabel('GDP Normalized')
        plt.ylabel('Article Count Normalized')
        plt.savefig("{}.png".format(query))

    # set x and y for regression
    y = df['count_z']
    x = df['gdp_z']

    # import pdb; pdb.set_trace()

    n_rows = len(y)
    slope, intercept, r_value, p_value, std_err = stats.linregress(x,y)
    # print("slope: ", slope)
    # print("intercept: ", intercept)
    # print("r_value: ", r_value)
    # print("p_value: ", p_value)
    # print("std_err: ", std_err)

    if not engine.dialect.has_table(engine, 'linnear_reg_results'):
        StatsResults.__table__.create(engine)

    results = StatsResults(
        query=query,
        source=source,
        n_rows=n_rows,
        slope=slope,
        intercept=intercept,
        r_value=r_value,
        p_value=p_value,
        std_err=std_err,
        date=datetime.datetime.now()
    )
    session = Session()
    session.add(results)

    session.commit()
    session.close()


def main():
    theme_list = [
        "TAX_FNCACT_WOMEN",
        'WOMEN',
        'JOBS'
    ]
    for theme in theme_list:
        for i in range(1): # Threads per theme
            q = QueryThread(country_queue, theme, Session)
            q.setDaemon = True
            q.start()
    # Build queue (set) of unique contry codes in entire data set
    # for every theme tested, count records for each unique country
    country_queue.join()

if __name__ == '__main__':
    country_queue = Queue()
    params = getDbParams(r"/Users/Jacobus/Documents/Database_Management/local_db_creds.json")
    engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(params['username'],params['password'],params['host'],params['port'],params['database_name']))
    Session = sessionmaker(bind=engine)

    createTable(Counts, engine)

    # populate the queue with unique contry codes
    try:
        countries = []
        session = Session()
        for row in session.query(Gdelt_v2.country_codes).distinct():
            countries.extend(row[0].split(','))
        countries = set(countries)
    except:
        session.rollback()
        raise
    finally:
        session.close()
    for c in countries: country_queue.put(c)
    main()
