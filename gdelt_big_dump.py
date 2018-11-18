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
    __tablename__ = "gdelt_v2_gkg_2018"
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
            while True:
                for line in response.iter_lines(chunk_size):
                    file_urls = re.findall(regex,str(line),re.IGNORECASE)
                    bytes_transfered+=chunk_size
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
                                    data = [re.split(br"\t",l) for l in v.split(b'\n') if (re.search(theme_regex.encode('utf-8'),l) and re.split(br"\t",l)[10])]
                                    if len(data) > 0:
                                        # add datat to the queue
                                        queue[url] = data
                                        condition.notify()
                                        condition.release()
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
            key, value = queue.popitem()
            value = [[i.decode('utf-8','strict') for i in row] for row in value]
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

if __name__ == '__main__':
    queue = {}
    MAX_QUEUE = 10
    condition = Condition()

    # Will match themes in each line of data.
    # For best results, use the entire theme code.
    theme_regex = r"TAX_FNCACT_WOMEN"

    params = getDbParams(r"/Users/Jacobus/Documents/Database_Management/local_db_creds.json")
    engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(params['username'],params['password'],params['host'],params['port'],params['database_name']))
    createTable(Gdelt_v2,engine)
    Session = sessionmaker(bind=engine)


    ProducerThread().start()
    # NOTE: Multiple producer threads can be started by just instantiating/calling
    # them, however, duplicate date will be produced.  Need some way of ensuring that
    # there is no overlap between producers.
    #   - pass in a date range parameter to each producer.
    #   - have producers work from a queue and be consumers of that queue.  Another
    #     producer will feed a bunch of csv urls to that queue.
    ConsumerThread().start()
