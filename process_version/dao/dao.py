from sqlalchemy import create_engine
from ..settings import settings
from ..models import Models

class Connection:

    engine = None
    models = None

    def __init__(self):
        if Connection.engine is None:
            Connection.engine = self.lazy_connection()
            models = Models()
            self.create_tables(models.metadata)
            Connection.models = models.models

    def lazy_connection(self):
        engine = '{engine}://{username}:{pwd}@{host}:{port}/{db}'.format(
           engine=settings.DB['ENGINE'],
           username=settings.DB['USER'],
           pwd=settings.DB['PASSWORD'],
           host=settings.DB['HOST'],
           port=settings.DB['PORT'],
           db=settings.DB['DATABASE'])
        return create_engine(engine,
                              echo=False,
                              isolation_level="READ UNCOMMITTED")


    def create_tables(self, metadata):
      metadata.create_all(Connection.engine)


class DAO:

    def __init__(self, lazy=True):
        con = Connection()
        if lazy:
            self.engine = con.lazy_connection()
        else:
            self.engine = con.engine
        self.models = con.models
        # self.raw_connection = self.engine.raw_connection()

    def create(self, obj):
        pass

    def find(self, obj):
        pass

    def update(self, obj):
        pass

    def delete(self, obj):
        pass

    def get_model(self, name):
        return self.models.get(name)

    def execute(self, statement):
        with self.engine.begin() as connection:
            #conn = self.engine.connect()
            connection.execute(statement)
