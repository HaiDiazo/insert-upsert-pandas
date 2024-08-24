from sqlalchemy import create_engine
from sqlalchemy import URL


class PostgresConfig:
    def __init__(self, **context):
        self.__database = context.get('pg_db')
        self.__host = context.get('pg_host')
        self.__port = context.get('pg_port')
        self.__user = context.get('pg_user')
        self.__pass = context.get('pg_pass')

    def client_connect(self):
        url_object = URL.create(
            "postgresql",
            username=self.__user,
            password=self.__pass,
            host=self.__host,
            database=self.__database,
            port=self.__port
        )
        db = create_engine(url_object)
        return db.connect()
