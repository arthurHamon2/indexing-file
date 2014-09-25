import csv
import requests
from .profiler import Profiler
from .dao.store import ItemDAO
from .models import Item

class CSV_TEST:

    HEADER = 1
    BATCH = 1000

    def __init__(self, url, delimiter=';'):
        self.file_url = url
        self.fields = []
        self.current_line = 0
        self.delimiter = delimiter
        self.p = Profiler()
        self.statements = []
        self.dao = ItemDAO()

    def execute(self):
        resp = requests.get(self.file_url, stream=True)
        temp = requests.get(self.file_url, stream=True)
        self.fields = self.init_fields(temp)
        for line in resp.iter_lines(chunk_size=512):
            # filter out keep-alive new lines
            if line  and self.current_line != 0:
                item = line.decode('utf-8')
                for row in csv.reader([item], delimiter=self.delimiter):
                    item_dic = {}
                    for field, val in zip(self.fields, row):
                        item_dic[field] = val
                    #print(item_dic['id'])
                    self.statements.append(Item(7, item_dic))
                    #self.dao.create(Item(6, item_dic))
                if len(self.statements) >= self.BATCH:
                    with self.p:
                        print('Insert in database:')
                        self.dao.create(self.statements)
                        self.statements = []
                self.current_line += 1
            if self.current_line == 0:
                self.current_line += 1
        with self.p:
            print('Insert in database:')
            self.dao.create(self.statements)
            self.statements = []
        print(self.current_line)

    def init_fields(self, response):
        fields = next(response.iter_lines(chunk_size=512))
        return next(csv.reader([fields.decode('utf-8')],
                                       delimiter=self.delimiter))
