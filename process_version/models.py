"""
Draft module which represents the database model.
"""
from sqlalchemy import Table, Column, Integer, MetaData
from sqlalchemy.dialects.postgresql import HSTORE


class Models:
    """
    Defines the tables in the database and if not present, they will be
    automaticaly created.
    """
    def __init__(self):
        self.metadata = MetaData()
        self.models = {}
        self.init_models()

    def init_models(self):
        """
        Defines all the tables in the database.
        """
        self.models['items'] = Table('items', self.metadata,
                                     Column('id', Integer, primary_key=True),
                                     Column('owner', Integer),
                                     Column('item', HSTORE))


class Item:
    """
    The item object represents the table in the DB.
    """
    def __init__(self, owner, item, _id=None):
        self._id = _id
        self.owner = owner
        self.item = item

    def serialize(self, out, delimiter=';'):
        """
        Serialize the object to a csv-like format.
        """
        out.write("{}{}{}\n".format(self.owner, delimiter,
                                    self._serialize_hstore()))

    def _serialize_hstore(self):
        """
        Serialize a dictionary into an hstore literal.
        Keys and values must both be strings (except None for values).
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
