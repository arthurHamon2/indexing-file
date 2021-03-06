import csv
import requests
from profiler import Profiler

class CSV_TEST:

    HEADER = 1

    def __init__(self, url, nb_consumer=3, queue_size=100):
        self.file_url = url
        self.fields = []
        self.current_line = 0
        self.delimiter = '|'

    #@profile
    def execute(self):
        resp = requests.get(self.file_url, stream=True)
        # for line in r.iter_lines(chunk_size=2048):
        #     # filter out keep-alive new lines
        #     if line:
        #         try:
        #             line = line.decode('latin-1')
        #         except Exception:
        #             print("exception: "+str(line))
        #         self.current_line += 1
        #         if self.current_line == self.HEADER:
        #             #import pdb; pdb.set_trace()
        #             for row in csv.reader([line], delimiter=';'):
        #                 self.fields = row
        #         else:
        #             for row in csv.reader([line], delimiter=';'):
        #                 csv_line = row
        #             item = {}
        #             for field in self.fields:
        #                 #print(field)
        #                 #import pdb; pdb.set_trace()
        #                 for value in csv_line:
        #                     item[field] = value
        #             #print(item)
        profiler = Profiler()
        for line in resp.iter_lines(chunk_size=512):
            # filter out keep-alive new lines
#            import pdb;pdb.set_trace()
            if line:
#                print(type(line))

                with profiler:
                    item = line.decode('utf-8')
#                print(type(item))
                for row in csv.reader([item], delimiter=self.delimiter):
                    item_dic = {}
                    if not self.fields:
                        self.fields = row
                        #print(self.fields)
                    else:
                        for field, val in zip(self.fields, row):
                            item_dic[field] = val
                        #print(item_dic)
                #print(item_dic)
        print(profiler)

    # #@profile
    # def test_iter_lines(self, response, chunk_size=512, decode_unicode=None):
    #     """Iterates over the response data, one line at a time.  When
    #     stream=True is set on the request, this avoids reading the
    #     content at once into memory for large responses.
    #     """

    #     pending = None

    #     for chunk in response.iter_content(chunk_size=chunk_size, decode_unicode=decode_unicode):

    #         if pending is not None:
    #             chunk = pending + chunk
    #         lines = chunk.splitlines()

    #         if lines and lines[-1] and chunk and lines[-1][-1] == chunk[-1]:
    #             pending = lines.pop()
    #         else:
    #             pending = None

    #         for line in lines:
    #             yield line

    #     if pending is not None:
    #         yield pending

    # def test_urllib3(self, url):
    #     import urllib3
    #     http = urllib3.PoolManager()
    #     r = http.request('GET', url)

    #     with open(path, 'wb') as out:
    #         while True:
    #             data = r.read(chunk_size)
    #             if data is None:
    #                 break
    #             out.write(data)

    #     r.release_conn()
