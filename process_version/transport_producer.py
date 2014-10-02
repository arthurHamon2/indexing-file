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
        self.content_length = int(self.response.headers['content-length'])

    def generator(self):
        """
        """
        for line in self.iter_lines(chunk_size=512):
            if line and self.current_line != 0:
                yield line
            self.current_line += 1

    def iter_lines(self, chunk_size=512, decode_unicode=None):
        """Iterates over the response data, one line at a time.  When
        stream=True is set on the request, this avoids reading the
        content at once into memory for large responses.
        """
        pending = None

        for chunk in self.response.iter_content(chunk_size=chunk_size, decode_unicode=decode_unicode):
            self.content_readen += chunk_size
            if pending is not None:
                chunk = pending + chunk
            lines = chunk.splitlines()

            if lines and lines[-1] and chunk and lines[-1][-1] == chunk[-1]:
                pending = lines.pop()
            else:
                pending = None
            for line in lines:
                yield line

        if pending is not None:
            yield pending

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
        with open(self.path, 'rb') as _file:
            for line in _file:
                self.content_readen = _file.tell()
                if line and self.current_line != 0:
                    yield line
                self.current_line += 1

    def get_csv_fields(self, delimiter, encoding):
        """
        Retrieves the first line of the csv to initialize the fields

        response -- the temporary response used to retrieve the csv header.
        """
        with open(self.path, 'rb') as temp_file:
            fields = temp_file.readline()
        return next(csv.reader([fields.decode(encoding)],
                               delimiter=delimiter))

