# import pdb; pdb.set_trace()
import requests, zipfile, io, re, csv, json
from threading import Thread, Condition
from queue import Queue
import time, random

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import *
from sqlalchemy import Column, Integer, Float, String
from sqlalchemy.orm import sessionmaker

# -- UTILITY FUNCTIONS ---------------------------------------------------------

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

# -- THREAD CLASSES ------------------------------------------------------------

class UrlProducerThread(Thread):
    def __init__(self,urlQueue, csv_regex):
        Thread.__init__(self)
        self.queue = urlQueue
        self.regex = csv_regex
    def run(self):
        ROOT_URL = "http://data.gdeltproject.org/gdeltv2/masterfilelist-translation.txt"
        chunk_size = 8096
        with requests.get(ROOT_URL,stream=True) as response:
            while True:
                for line in response.iter_lines(chunk_size):
                    file_urls = re.findall(self.regex,str(line),re.IGNORECASE)
                    for url in file_urls:
                        urlQueue.put(url)


class DataProducerThread(Thread):
    def __init__(self,queue, urlQueue, theme_regex):
        Thread.__init__(self)
        self.queue = queue
        self.urlQueue = urlQueue
        self.theme_regex = theme_regex
    def run(self):
        # global queue
        # ROOT_URL = "http://data.gdeltproject.org/gdeltv2/masterfilelist-translation.txt"
        # regex = r"http://.*2017\d{10}.*\.gkg\.csv\.zip"
        chunk_size = 8096
        bytes_transfered = 0
        # with requests.get(ROOT_URL,stream=True) as response:
        while True:
            url = self.urlQueue.get()
            r = requests.get(url)
            if r.status_code == 200:
                with io.BytesIO() as f:
                    f.write(r.content)
                    extracted = extract_zip(f)
                    for k,v in extracted.items():
                        data = [re.split(br"\t",l) for l in v.split(b'\n') if (re.search(self.theme_regex.encode('utf-8'),l) and re.split(br"\t",l)[10])]
                        if len(data) > 0:
                            # add datat to the queue
                            self.queue.put(data)
                            print("Producer finished...")
            else:
                print("Could not connect to server (status code = {}) at {}.".format(r.status_code, url))
            # unzip every gkg file in memory
            # loop through lines in file, stream fassion
            # see: https://stackoverflow.com/questions/22340265/python-download-file-using-requests-directly-to-memory

class ConsumerThread(Thread):
    def __init__(self,queue):
        Thread.__init__(self)
        self.queue = queue
    def run(self):
        while True:
            value = self.queue.get()
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
                print("Consumer finished...")
                self.queue.task_done()

# -- MAIN CONTROLLER -----------------------------------------------------------

def main():

    # global queue
    # global urlQueue

    # Will match themes in each line of data.
    # For best results, use the entire theme code.
    theme_regex = r"TAX_FNCACT_WOMEN"
    csv_regex = r"http://.*2017\d{10}.*\.gkg\.csv\.zip"

    for i in range(4):
        u = UrlProducerThread(urlQueue, csv_regex)
        u.setDaemon = True
        u.start()

    for i in range(20):
        p = DataProducerThread(queue,urlQueue, theme_regex)
        p.setDaemon = True
        p.start()

    for i in range(3):
        c = ConsumerThread(queue)
        c.setDaemon = True
        c.start()

    queue.join()

if __name__ == '__main__':
    queue = Queue()
    urlQueue = Queue()

    params = getDbParams(r"/Users/Jacobus/Documents/Database_Management/local_db_creds.json")
    engine = create_engine('postgresql://{}:{}@{}:{}/{}'.format(params['username'],params['password'],params['host'],params['port'],params['database_name']))
    createTable(Gdelt_v2,engine)
    Session = sessionmaker(bind=engine)

    main()
