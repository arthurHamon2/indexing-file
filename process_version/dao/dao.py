"""
This module defines the Data Access Object layer,
also providing the connection.
"""
from sqlalchemy import create_engine
from ..settings import settings
from ..models import Models


class Connection:
    """
    Provide a connection in two different ways:
    - As a singleton, the connection is initialized only once.
    - A "lazy connection" that provides a connection even if one has already
      been initialized.
    """
    engine = None
    models = None

    def __init__(self):
        if Connection.engine is None:
            Connection.engine = self.lazy_connection()
            models = Models()
            self.create_tables(models.metadata)
            Connection.models = models.models

    def lazy_connection(self):
        """
        Initialize a connection returning an engine object.
        """
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
        """
        Creates the tables in the database.
        """
        metadata.create_all(Connection.engine)


class DAO:
    """
    DAO abstraction layer, it only retrieves a connection from the database.
    Note that, by default, every DAO instance creates a connection to
    the database. This behaviour can be changed by turning proc_behaviour
    to False.
    The CRUD operations are abstract and must be overrided by the
    implementation classes
    """
    def __init__(self, proc_behaviour=True):
        con = Connection()
        if proc_behaviour:
            self.engine = con.lazy_connection()
        else:
            self.engine = con.engine
        self.models = con.models

    def get_model(self, name):
        """
        Return the model with the given "name".
        """
        return self.models.get(name)

    def execute(self, statement):
        """
        Execute a statement and return the result
        """
        connection = self.engine.connect()
        result = connection.execute(statement)
        connection.close()
        return result.fetchall()

    def transaction(self, statement):
        """
        Execute a transaction with the given statement.
        """
        with self.engine.begin() as connection:
            connection.execute(statement)

    def create(self, obj):
        pass

    def find(self, obj):
        pass

    def update(self, obj):
        pass

    def delete(self, obj):
        pass
