# import pdb; pdb.set_trace()
import requests, zipfile, io, re, csv, json
from threading import Thread, Condition
import time, random

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import *
from sqlalchemy import Column, Integer, Float, String
from sqlalchemy.orm import sessionmaker

def extract_zip(input_zip):
    input_zip = zipfile.ZipFile(input_zip)
    return {i: input_zip.read(i) for i in input_zip.namelist()}

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
class Gdelt_v2(Base):
    __tablename__ = "gdelt_v2_2018"
    id = Column(Integer, primary_key=True)
    gkgrecordid = Column(String)
    date = Column(String)
    document_identifier = Column(String)
    v2_themes = Column(String)
    v2_locations = Column(String)
    v2_tone = Column(String)


class ProducerThread(Thread):
    def run(self):
        global queue
        ROOT_URL = "http://data.gdeltproject.org/gdeltv2/masterfilelist-translation.txt"
        regex = r"http://.*2017\d{10}.*\.gkg\.csv\.zip"
        chunk_size = 8096
        bytes_transfered = 0
        with requests.get(ROOT_URL,stream=True) as response:
        # response = requests.get(ROOT_URL,stream=True)
        # queue = {}
            while True:
                for line in response.iter_lines(chunk_size):
                    # import pdb; pdb.set_trace()
                    file_urls = re.findall(regex,str(line),re.IGNORECASE)
                    bytes_transfered+=chunk_size
                    # print("bytes transfered: {}".format(bytes_transfered))
                    for url in file_urls:
                        condition.acquire()
                        if len(queue) == MAX_QUEUE:
                            print("Queue full...waiting on consumer.")
                            condition.wait()
                            print("Prudocer notified...continuing.")
                        r = requests.get(url)
                        if r.status_code == 200:
                            with io.BytesIO() as f:
                                f.write(r.content)
                                extracted = extract_zip(f)
                                for k,v in extracted.items():
                                    # TODO: add condition to allow only lines with location data
                                    data = [re.split(br"\t",l) for l in v.split(b'\n') if re.search(br"TAX_FNCACT_WOMEN",l)]
                                    if len(data) > 0:
                                        queue[url] = data
                                        condition.notify()
                                        condition.release()
                                        # print("Results found: {}".format(len(results.keys())))
                        else:
                            print("Could not connect to server (status code = {}) at {}.".format(r.status_code, url))
                    # unzip every gkg file in memory
                    # loop through lines in file, stream fassion
                    # see: https://stackoverflow.com/questions/22340265/python-download-file-using-requests-directly-to-memory

class ConsumerThread(Thread):
    def run(self):
        global queue
        while True:
            condition.acquire()
            if not queue:
                print("Nothing in queue...consumer waiting.")
                condition.wait()
                print("Item(s) in queue... continuing.")
            # map of row schema/indexes
            # fieldIndex = {
            #     'gkgrecordid':0,
            #     'date':1,
            #     'document_identifier':4,
            #     'v2_themes':8,
            #     'v2_locations':10,
            #     'v2_tone':15
            # }
            key, value = queue.popitem()
            value = [[i.decode('utf-8','strict') for i in row] for row in value]
            # data = {k:[] for k in fieldIndex.keys()}
            # for row in value:
            session = Session()
            try:
                session.add_all(
                    [Gdelt_v2(gkgrecordid=row[0],date=row[1],document_identifier=row[4],v2_themes=row[8],v2_locations=row[10],v2_tone=row[15]) for row in value]
                )
                session.commit()
            except:
                session.rollback()
                raise
            finally:
                session.close()


                # for k,v in fieldIndex:
                #     data[k].append(row[v])
            # TODO: do something with the csv url path... make special table?

            # upload data to table?
            # or just count it and collect counted data...?
                # data = {key:row[fieldIndex[key]] for key in fieldIndex.keys()}

if __name__ == '__main__':
    queue = {}
    MAX_QUEUE = 10
    condition = Condition()


    params = getDbParams(r"/Users/Jacobus/Documents/Database_Management/local_db_creds.json")
    engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(params['username'],params['password'],params['host'],params['port'],params['database_name']))
    createTable(Gdelt_v2,engine)
    Session = sessionmaker(bind=engine)


    ProducerThread().start()
    ConsumerThread().start()
