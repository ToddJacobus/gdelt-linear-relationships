# import pdb; pdb.set_trace()
import requests, zipfile, io, re, csv
from threading import Thread, Condition
import time, random

def extract_zip(input_zip):
    input_zip = zipfile.ZipFile(input_zip)
    return {i: input_zip.read(i) for i in input_zip.namelist()}

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
            del queue[list(queue.keys())[0]]
            time.sleep(random.random())

if __name__ == '__main__':
    queue = {}
    MAX_QUEUE = 10
    condition = Condition()

    ProducerThread().start()
    ConsumerThread().start()
