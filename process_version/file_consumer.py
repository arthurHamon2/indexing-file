import csv
import math
import chardet
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

    DELIMITERS = ['\t', ',', ';', '|']

    def __init__(self, queue, encoding, **kwargs):
        """
        CSV constructor.

        queue -- The queue used by the producer.
        """
        super().__init__(queue)
        
        self.encoding = encoding
        # Initialisation of the csv fields
        self.fields = kwargs['fields']
        self.dialect = kwargs['dialect']
        self.content_length = kwargs['content_length']

        self.dao = ItemDAO()
        self.batch = self._init_batch(self.content_length)
        self.statements = []

    def _init_batch(self, content_length):
        """
        Determines the batch according to the content-length
        given by the request's header.

        content_length -- the response size in bytes.
        """
        content_length = math.ceil(content_length / (1000 * 1000))
        batch = (content_length * self.BATCH_MIN) / self.LENGTH_MIN
        batch = self.BATCH_MIN if batch < self.BATCH_MIN else batch
        batch = self.BATCH_MAX if batch > self.BATCH_MAX else batch
        print("batch size: {}".format(batch))
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
        for row in csv.DictReader([item], fieldnames=self.fields, dialect=self.dialect):
            # Construct a dictionary of a csv line.
            #FIXME: parsing issue !
            self.statements.append(Item(7, row))
        if len(self.statements) >= self.batch:
            self.insert_lines()
        return row

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
            #print('Insert in database:')
            self.dao.copy(self.statements)
            self.statements = []

    @classmethod
    def get_csv_info(cls, field_line, encoding, delimiter=None):
        if delimiter is None:
            delimiter = CSV.DELIMITERS
        if type(delimiter) is not list:
            delimiter = [delimiter]
        field_line = field_line.decode(encoding)
        dialect = csv.Sniffer().sniff(field_line, delimiters=delimiter)
        fields = next(csv.reader([field_line], dialect))
        return fields, dialect

    @classmethod
    def get_encoding(cls, sample):
        chardetect = chardet.detect(sample)
        print('encoding: {} with confidence {}'.format(chardetect['encoding'],
                                                       chardetect['confidence']))
        return chardetect['encoding']