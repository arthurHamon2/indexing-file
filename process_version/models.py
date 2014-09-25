import csv
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey
from sqlalchemy.dialects.postgresql import HSTORE

class Models:

    def __init__(self):
        self.metadata = MetaData()
        self.models = {}
        self.init_models()

    def init_models(self):
        self.models['items'] = Table('items', self.metadata,
                                     Column('id', Integer, primary_key=True),
                                     Column('owner', Integer),
                                     Column('item', HSTORE))

class Item:

    def __init__(self, owner, item, _id=None):
        self._id = _id
        self.owner = owner
        self.item = item

    def serialize(self, out, delimiter=';'):
        #print(self._serialize_hstore())
        # writer = csv.writer(out, delimiter=";", quotechar='', quoting=csv.QUOTE_NONE)
        # writer.writerow([self.owner, self._serialize_hstore()])
        out.write("{}{}{}\n".format(self.owner, delimiter,
                                    self._serialize_hstore()))

    def hstore_repr(self):
        rep = ''
        for key, value in self.item.items():
            rep += '\'{}\' => \'{}\','.format(key, value)
        rep = rep[:-1]
        # rep += '\''
        return rep

    def _serialize_hstore(self):
        """
        Serialize a dictionary into an hstore literal.  Keys and values must
        both be strings (except None for values).
        """
        def esc(s, position):
            if position == 'value' and s is None:
                return 'NULL'
            elif isinstance(s, str):
                return '"%s"' % s.replace('"', r'\'')
            else:
                raise ValueError("%r in %s position is not a string." %
                                 (s, position))

        return ', '.join('%s=>%s' % (esc(k, 'key'), esc(v, 'value'))
                         for k, v in self.item.items())

    def __str__(self):
        return 'id : {}, owner : {}, item : {}'.format(self._id,
                                                       self.owner,
                                                       self.item)
