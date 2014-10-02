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

def test_copy_store():
    item = Item(1, {'\'\"field1,|""\'\'': 'value,|"\''})
    dao = ItemDAO()
    assert dao.copy([item]) == True

def test_find_store():
    item = Item(5, {'field1': 'value1', 'field4': 'value4'})
    dao = ItemDAO()
    dao.create(item)
    result_v = False
    for result in dao.find_by_owner(item.owner):
        result_v = True
        assert result[dao.items.c.item] == item.item
    assert result_v is True
