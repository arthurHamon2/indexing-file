from ..dao.dao import DAO
from ..dao.store import ItemDAO
from ..models import Item

def test_connection():
    dao = DAO()

def test_insert_store():
    item = Item(1, {'field': 'value', 'field1': 'value1'})
    item1 = Item(2, {'field': 'value', 'field1': 'value1'})
    dao = ItemDAO()
    dao.create([item, item1])
