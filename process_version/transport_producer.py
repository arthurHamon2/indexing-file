import os
import csv
import requests
from .consume_file import Producer


class Request(Producer):
    """
    """
    def __init__(self, url, queue, nb_consumer):
        """
        Request constructor.

        queue -- The queue to communicate with the consumers.
        nb_consumer -- The number of consumer that will be started.
        """
        super().__init__(queue, nb_consumer)
        self.url = url
        self.response = requests.get(url, stream=True)
        self.content_length = self.response.headers['content-length']

    def generator(self):
        """
        """
        for line in self.response.iter_lines(chunk_size=512):
            # filter out keep-alive new lines
            if line and self.current_line != 0:
                yield line
            self.current_line += 1

    def get_csv_fields(self, delimiter, encoding):
        """
        Retrieves the first line of the csv to initialize the fields

        response -- the temporary response used to retrieve the csv header.
        """
        temp_response = requests.get(self.url, stream=True)
        fields = next(temp_response.iter_lines(chunk_size=512))
        return next(csv.reader([fields.decode(encoding)],
                               delimiter=delimiter))


class FileSystem(Producer):
    """
    """
    def __init__(self, path, queue, nb_consumer):
        """
        FileSystem constructor.

        queue -- The queue to communicate with the consumers.
        nb_consumer -- The number of consumer that will be started.
        """
        super().__init__(queue, nb_consumer)
        self.path = path
        self.content_length = os.path.getsize(path)

    def generator(self):
        """
        """
        with open(self.path, 'r') as _file:
            for line in _file:
                if line and self.current_line != 0:
                    yield line
                self.current_line += 1

    def get_csv_fields(self):
        """
        Retrieves the first line of the csv to initialize the fields

        response -- the temporary response used to retrieve the csv header.
        """
        with open(self.path, 'r') as temp_file:
            fields = temp_file.readline()
        return next(csv.reader([fields.decode(self.encoding)],
                               delimiter=self.delimiter))
