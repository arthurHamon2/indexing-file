"""
Module which describes the DAO for the item object.
"""
from sqlalchemy.sql import select
from .dao import DAO
import io

class ItemDAO(DAO):
    """
    DAO layer for the item object.
    """
    def __init__(self):
        super().__init__()
        self.items = super().get_model('items')

    def create(self, objects):
        """
        Create a list of items in the DB and store the item attribute as
        a HSTORE field.
        """
        if not type(objects) == list:
            objects = [objects]
        values = [{'owner': x.owner, 'item': x.item} for x in objects]
        objects = None
        super().transaction(self.items.insert().values(values))

    def copy(self, objects, delimiter=';'):
        """
        Extra method which inserts items in the DB, like the create method,
        but faster.
        """
        output = io.StringIO()
        # Serialize the objects as a csv-like file in the output buffer.
        for obj in objects:
            obj.serialize(output, delimiter=delimiter)
        # Get a raw connection to manipulate a cursor.
        conn = self.engine.raw_connection()
        cur = conn.cursor()
        # Rewind the output
        output.seek(0)
        try:
            cur.copy_from(output, "items", sep=delimiter,
                          columns=("owner", "item"))
            # Commit the result.
            conn.commit()
        except Exception as err:
            conn.rollback()
            print("Caught error: {}\n".format(err))
        # Close the cursor, connection and the buffer.
        cur.close()
        output.close()
        conn.close()

    def find_by_owner(self, owner):
        statement = select([self.items]).where(self.items.c.owner == owner)
        return super().execute(statement)

    def update(self, obj):
        pass

    def delete(self, obj):
        pass


