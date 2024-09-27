from sqlalchemy import create_engine, CursorResult
from sqlalchemy.sql.expression import select, text
from typing import Literal
from sqlalchemy.engine import Engine, Connection
from sqlalchemy.exc import OperationalError, SQLAlchemyError
import logging

logger = logging.getLogger(__name__)
logging.basicConfig()
logger.setLevel(logging.INFO)


class SqlAlchemyUtil:
    def __init__(
            self,
            connection_string: Literal["{diver}://{user}@{host}:{port}/{catalog}"],

    ) -> None:
        self.conection_string = connection_string
        self.engine = None
        self.__connection: Connection = None

    def connect(self):
        try:
            if self.__connection is None:
                self.engine = create_engine(self.conection_string)
                self.__connection = self.engine.connect()
        except OperationalError as e:
            logging.warning("[SQLAlchemy] Connection failed: %s. Retry connection!", e)
            try:
                self.engine = create_engine(self.conection_string)
                self.__connection = self.engine.connect()
            except OperationalError as e:
                logger.error(str(e))
                raise e

    def execute_query(self, query: str):
        """Execute a raw SQL query with manual transaction control."""
        trans = None
        if self.__connection is None:
            self.connect()
        try:
            trans = self.__connection.begin()  # Begin a transaction
            self.__connection.execute(text(query))
            trans.commit()  # Commit the transaction
            logging.info("Query executed successfully.")
        except SQLAlchemyError as e:
            trans.rollback()  # Roll back the transaction on error
            logging.error(f"Error occurred: {e}")
            raise e  # Re-raise the exception for further handling if needed

    def execute_multiple_queries(self, queries: list):
        # Ensure to connect to the database
        self.connect()

        # Start a transaction
        with self.__connection.begin() as transaction:
            try:
                for query in queries:
                    if query.strip():  # Check if the query is not empty
                        self.__connection.execute(text(query))
                # Commit the transaction if all queries are successful
                transaction.commit()
            except OperationalError as e:
                # Log the error and roll back the transaction
                logger.error(f"[SQLAlchemy] Execute failed due to: {str(e)}")
                transaction.rollback()
                raise e
            except Exception as e:
                # Roll back the transaction for any other exceptions
                transaction.rollback()
                raise e
            finally:
                # Always close the connection
                self.__connection.close()

    def execute_count_query(self, query: str):
        self.connect()
        try:
            result = self.__connection.execute(text(query))
            count = result.scalar()
            return count
        except Exception as e:
            logger.error(str(e))
            raise e
        finally:
            self.__connection.close()

    def execute_query_to_get_data(self, query: str):
        self.connect()
        try:
            result: CursorResult = self.__connection.execute(text(query))
            rows = result.fetchall()
            columns = result.keys()

            data = [dict(zip(columns, row)) for row in rows]
            return data
        except Exception as e:
            logger.error(str(e))
            raise e
        finally:
            self.__connection.close()
