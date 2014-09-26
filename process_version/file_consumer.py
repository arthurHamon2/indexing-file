import csv
import math

from .models import Item
from .dao.store import ItemDAO
from .consume_file import Consumer


class CSV(Consumer):
    """
    """

    # The first line of a csv file is a header
    HEADER = 1
    # Constant variables in order to compute a batch to proceed multiple lines
    # at once.
    LENGTH_MIN = 7  # in MB
    BATCH_MIN = 1000
    BATCH_MAX = 10000

    def __init__(self, fields, delimiter, content_length,
                 queue, encoding=None):
        """
        CSV constructor.

        queue -- The queue used by the producer.
        """
        super().__init__(queue)
        self.delimiter = delimiter
        self.encoding = encoding
        # Initialisation of the csv fields
        self.fields = fields

        self.dao = ItemDAO()
        self.batch = self._init_batch(content_length)
        self.statements = []

    def _init_batch(self, content_length):
        """
        Determines the batch according to the content-length
        given by the request's header.

        content_length -- the response size in bytes.
        """
        content_length = math.ceil(int(content_length) / (1000 * 1000))
        batch = (content_length * self.BATCH_MIN) / self.LENGTH_MIN
        batch = self.BATCH_MIN if batch < self.BATCH_MIN else batch
        batch = self.BATCH_MAX if batch > self.BATCH_MAX else batch
        print("BATCH = {}".format(batch))
        return batch

    def consume(self, item):
        """
        Process a csv line. Create an object and store it in a list.
        Once the length of the list reaches the batch limit, it flushes all the
        data stored in the database.

        item -- the item retrieved from the producer queue.
        """
        try:
            item = item.decode(self.encoding)
        except UnicodeError:
            print("unicode error on: {}".format(item))
        for row in csv.reader([item], delimiter=self.delimiter):
            # item_dic = {}
            # Construct a dictionary of a csv line.
            item_dic = {field: val for field, val in zip(self.fields, row)}
            # for field, val in zip(self.fields, row):
            #     item_dic[field] = val
            self.statements.append(Item(7, item_dic))
        if len(self.statements) >= self.batch:
            self.insert_lines()
        return item_dic

    def exit_operation(self):
        """
        Proceed the last data before exiting the process.
        """
        self.insert_lines()

    def insert_lines(self):
        """
        Insert the data in the database.
        """
        with self.p:
            print('Insert in database:')
            self.dao.copy(self.statements, delimiter=self.delimiter)
            self.statements = []
