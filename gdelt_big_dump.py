# import pdb; pdb.set_trace()
import requests, zipfile, io, re, csv
from threading import Condition

def extract_zip(input_zip):
    input_zip = zipfile.ZipFile(input_zip)
    return {i: input_zip.read(i) for i in input_zip.namelist()}

def main():
    ROOT_URL = "http://data.gdeltproject.org/gdeltv2/masterfilelist-translation.txt"
    regex = r"http://.*2017\d{10}.*\.gkg\.csv\.zip"
    chunk_size = 8096
    bytes_transfered = 0
    response = requests.get(ROOT_URL,stream=True)

    queue = {}
    for line in response.iter_lines(chunk_size):
        # import pdb; pdb.set_trace()
        file_urls = re.findall(regex,str(line),re.IGNORECASE)
        bytes_transfered+=chunk_size
        # print("bytes transfered: {}".format(bytes_transfered))

        for url in file_urls:
            r = requests.get(url)
            if r.status_code == 200:
                with io.BytesIO() as f:
                    f.write(r.content)
                    extracted = extract_zip(f)
                    for k,v in extracted.items():
                        data = [re.split(br"\t",l) for l in v.split(b'\n') if re.search(br"TAX_FNCACT_WOMEN",l)]
                        if len(data) > 0:
                            queue[url] = data
                            print("Added result to queue...")
                            # print("Results found: {}".format(len(results.keys())))
            else:
                print("Could not connect to server (status code = {}) at {}.".format(r.status_code, url))
        # unzip every gkg file in memory
        # loop through lines in file, stream fassion
        # see: https://stackoverflow.com/questions/22340265/python-download-file-using-requests-directly-to-memory

if __name__ == '__main__':
    main()
