# import pdb; pdb.set_trace()
import requests, zipfile, io, re, csv

def extract_zip(input_zip):
    input_zip = zipfile.ZipFile(input_zip)
    return {i: input_zip.read(i) for i in input_zip.namelist()}

def main():
    ROOT_URL = "http://data.gdeltproject.org/gdeltv2/masterfilelist-translation.txt"
    regex = r"http://.*\.gkg\.csv\.zip"
    chunk_size = 8096
    bytes_transfered = 0
    response = requests.get(ROOT_URL,stream=True)

    results = {}
    # for chunk in response.iter_content(chunk_size):
    for line in response.iter_lines(chunk_size, decode_unicode=False):
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
                        # csv_reader = csv.reader(v,delimiter=' ',quotechar='"')
                        # import pdb; pdb.set_trace()
                        # NOTE: this data feed is tab delimited, not space delimited...
                        # the source of all my woes...
                        # data = [re.split(r"\s{1}",l) for l in v.split('\n') if re.search(r"TAX_FNCACT_WOMEN",l)]
                        data = [re.split(r"\t",l) for l in v.split('\n') if re.search(r"TAX_FNCACT_WOMEN",l)]
                        if len(data) > 0:
                            results[url] = data
                            print("Results found: {}".format(len(results.keys())))
                            import pdb; pdb.set_trace()
            else:
                print("Could not connect to server (status code = {}) at {}.".format(r.status_code, url))
        # unzip every gkg file in memory
        # loop through lines in file, stream fassion
        # see: https://stackoverflow.com/questions/22340265/python-download-file-using-requests-directly-to-memory

if __name__ == '__main__':
    main()
