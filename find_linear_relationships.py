# import pdb; pdb.set_trace()
import pandas
import requests
import json, re, datetime
from shapely.geometry import Point
import geopandas as gpd
from geopandas import GeoSeries, GeoDataFrame

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from geoalchemy2 import Geometry, WKTElement
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.automap import automap_base
from sqlalchemy import *

# to do a DROP CASCADE
from sqlalchemy.schema import DropTable
from sqlalchemy.ext.compiler import compiles

import scipy.stats as stats
import matplotlib.pyplot as plt

# This recompiles the DropTable function so that it sends out DROP CASCADE sql code
# see: https://stackoverflow.com/questions/38678336/sqlalchemy-how-to-implement-drop-table-cascade
@compiles(DropTable, "postgresql")
def _compile_drop_table(element, compiler, **kwargs):
    return compiler.visit_drop_table(element) + " CASCADE"

# ------------------------------------------------------------------------------
# PARAMETERS
# define table name... I'll probably depricate this.
tableName = 'gdelt_v2_temp'
source = "gdelt_v2_api"
# Regex pattern by which to match GDELT themes in text document (see link below)
theme_pattern = r""
# GDELT themes text endpoint
themes_url = "http://data.gdeltproject.org/api/v2/guides/LOOKUP-GKGTHEMES.TXT"
# where the plots go...
path_to_plots = "/Users/Jacobus/Documents/Courses/ind_study_2018_msu/scripts/relationship_mining/output_plots/"
# Set to True to plot data for each iteration
plot = True

# will eventually depricate this...
# list of queries to loop through and build requests for GDELT
queries = [
    # 'theme:NATURAL_DISASTER_HURRICANE',
    # 'theme:TAX_FNCACT_WOMEN',
    # 'theme:TAX_FNCACT_LEADER',
    'theme:WB_2670_JOBS',
    "(women OR woman OR girl OR girls)",
    # "(man OR men OR boy OR boys)",
    # "(job OR jobs OR career OR careers)",
    # "ALL"
]

path_to_queryies_list = "/Users/Jacobus/Documents/Courses/ind_study_2018_msu/data/themes_under_001P_150n.csv"
# set to True if you want to define a list of themes from a text file
use_list = True
# ------------------------------------------------------------------------------


# sqlalchemy definitions
# NOTE: if a table does not have a primary key, it will not be mapped automatically
Base = automap_base()
engine = create_engine('postgresql://Jacobus@localhost:5432/postgres')
Base.prepare(engine,reflect=True)

# Make a new base because things get wierd when automap_base finds the existing
# table.
DBase = declarative_base()
class StatsResults(DBase):
    __tablename__ = "linnear_reg_results"
    id = Column(Integer, primary_key=True)
    query = Column(String)
    source = Column(String)
    n_rows = Column(Integer)
    slope = Column(Float)
    intercept = Column(Float)
    r_value = Column(Float)
    p_value = Column(Float)
    std_err = Column(Float)
    date = Column(Date)

# instantiate SQLAlchemy class mapped to world polygons table
Countries = Base.classes.world_country_polygons

# Get a session...
Session = sessionmaker(bind=engine)

# request and parse out list of themes that match theme_pattern
r = requests.get(themes_url)
total_theme_list = [i.split('\\')[0] for i in str(r.content).split('\\n') if re.search(theme_pattern,i, re.IGNORECASE)]



# check if the linnear_reg_results table exists and, if so, grab all the themes
# that have been done.
if use_list:
    # use list
    with open(path_to_queryies_list, "r") as file:
        theme_list = [line.strip().strip('"') for line in file]
else:
    if engine.dialect.has_table(engine, 'linnear_reg_results'):
        # create session in order to query themes already done
        session = Session()
        RegResults = Base.classes.linnear_reg_results
        done_list = []
        for regResults in session.query(RegResults).all():
            done_list.append(str(regResults.query))
        # create 'todo-list' of themes that have not been done yet.
        theme_list = [t for t in total_theme_list if t not in done_list]
        session.close()
    else:
        theme_list = total_theme_list

# Loop through query list and get data from gdelt
current = 1
starttime = datetime.datetime.now()
print("Started at {}...".format(starttime))
for query in queries:
    print("Analyzing theme {} of {}: {}".format(current,len(theme_list),query))
    current+=1
    # url = "https://api.gdeltproject.org/api/v2/geo/geo?query=theme:{}&format=GeoJSON&sortby=Date".format(query)
    if query == "ALL":
        url = "https://api.gdeltproject.org/api/v2/geo/geo?query=toneabs>0&format=GeoJSON&sortby=Date"
    else:
        url = "https://api.gdeltproject.org/api/v2/geo/geo?query={}&format=GeoJSON&sortby=Date".format(query)
    r = requests.get(url)
    if r.status_code == 200:
        try:
            data = r.json()
        except Exception as e:
            print(type(e),": ",e)
            continue
    else:
        print("Could not connect: {}".format(r.status_code))
        continue
    if len(data['features']) < 2:
        print("1 or fewer features found for {}...".format(query))
        continue

    # Flatten JSON into pandas-friendly dictionary
    flat_dictionary = {}
    for feature in data['features']:
        for key, value in feature.items():
          if type(feature[key]) == dict:
            for k, v in feature[key].items():
                if k in flat_dictionary.keys():
                    flat_dictionary[k].append(feature[key][k])
                else:
                    flat_dictionary[k] = [v]

    # Make pandas dataframe with data
    df = pandas.DataFrame.from_dict(flat_dictionary)
    df[['lon','lat']] = pandas.DataFrame(df.coordinates.values.tolist(),index = df.index)

    # Parse out geometry from lat/lon columns
    geom = [Point(xy) for xy in zip(df['lon'],df['lat'])]

    # Make GeoDataFrame
    gdf = GeoDataFrame(df,geometry=geom)

    # convert lat/lon to database-friendly WKT
    gdf['geometry'] = gdf['geometry'].apply(lambda x: WKTElement(x.wkt,srid=4326))

    # if table exists, drop it and add fresh data
    if engine.dialect.has_table(engine, tableName):
        GdeltTemp = Base.classes.gdelt_v2_temp
        GdeltTemp.__table__.drop(engine)

    # Upload to postgres
    gdf.to_sql(tableName,engine, if_exists='replace',index=False,
        dtype={
            'geometry': Geometry('POINT',srid=4326)
        })

    # Add primary key because pandas does not do this automatically...
    # Sort of a pandas limitation.
    engine.execute("ALTER TABLE gdelt_v2_temp ADD COLUMN id SERIAL PRIMARY KEY;")

    # SQL to count the number of mentions (in this case) for each country in the
    # data set.  TODO: Make this more pluggable onto other datasets.
    # Count number of articles that mention a given country.
    sql = """
        CREATE TABLE gdelt_count_temp as
        SELECT w.gid, sum(g.count)
        FROM world_country_polygons as w
        	LEFT JOIN {} as g
        	on ST_Contains(w.geom, g.geometry)
        GROUP BY w.gid;
        ALTER TABLE gdelt_count_temp ADD COLUMN id SERIAL PRIMARY KEY;
    """.format(tableName)

    # if gdelt_count_temp table exists, drop it, then run sql with fresh data.
    if engine.dialect.has_table(engine, 'gdelt_count_temp'):
        GdeltCountTemp = Base.classes.gdelt_count_temp
        GdeltCountTemp.__table__.drop(engine)
    engine.execute(sql)

    # Grab new tables and map SQLAlchemy Classes.
    Base.prepare(engine,reflect=True)
    PointCount = Base.classes.gdelt_count_temp
    GDP = Base.classes.world_gdp

    # check out session
    session = Session()

    # make dictionary to hold results in pandas-friendly format
    results = {
        'count':[],
        'gdp':[],
        'country_code':[]
    }

    # loop through and grab results from query result.
    for count, gdp in session.query(PointCount, GDP).\
                                filter(PointCount.gid == GDP.country_code).\
                                filter(PointCount.sum != None).\
                                filter(GDP.gdp_2017 != None).\
                                all():
                            results['count'].append(int(count.sum))
                            results['gdp'].append(float(gdp.gdp_2017))
                            results['country_code'].append(str(gdp.country_code))

    session.close()

    df = pandas.DataFrame.from_dict(results)

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

endtime = datetime.datetime.now()
print("Finished at {}...".format(endtime))
print("Elapsed time: {}.".format(endtime - starttime))
