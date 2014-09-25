from .dao import DAO
import io

class ItemDAO(DAO):

    def __init__(self):
        super().__init__()
        self.items = super().get_model('items')

    def create(self, objects):
        if not type(objects) == list:
            objects = [objects]
        values = [{'owner': x.owner, 'item': x.item} for x in objects]
        objects = None
        super().execute(self.items.insert().values(values))

    def copy(self, objects, delimiter=';'):
        output = io.StringIO()
        for obj in objects:
            obj.serialize(output, delimiter=delimiter)
        conn = self.engine.raw_connection()
        cur = conn.cursor()
        output.seek(0)
        try:
            cur.copy_from(output, "items", sep=delimiter,
                          columns=("owner", "item"))
        except Exception as err:
            conn.rollback()
            cur.close()
            output.close()
            print("Caught error:\n", err)
            return
        conn.commit()
        conn.close()
        cur.close()
        output.close()

    def find(self, obj):
        pass

    def update(self, obj):
        pass

    def delete(self, obj):
        pass


