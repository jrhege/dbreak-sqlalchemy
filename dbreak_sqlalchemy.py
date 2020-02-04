""" ConnectionWrapper for SQLAlchemy database console access """

from typing import List

import sqlalchemy

import dbreak.connections
import dbreak.outputs


class SQLAlchemyWrapper(dbreak.connections.ConnectionWrapper):
    """ Wrapper for SQLAlchemy engine objects """

    HELP_TEXT = "This connection accepts SQL commands. Enter SQL as you would in any other database client."

    SEARCH_RANK = 1

    def execute_statement(self, statement: str) -> List:
        """ Return the results of executing a database statement

        :param statement: Statement to execute in the database
        """

        # Execute the statement and get a ResultProxy for the results
        result_proxy = self.raw_connection.execute(statement)

        try:
            outputs = self._parse_response(
                result_proxy=result_proxy
            )
        finally:
            result_proxy.close()

        return outputs

    def _parse_response(self, result_proxy: sqlalchemy.engine.ResultProxy) -> List:
        """ Parse a ResultProxy object into a list of outputs

        :param result_proxy: SQLAlchemy ResultProxy to pull rows from
        """

        # Attempt to read the columns that appear in the
        # resultset. This won't return anything for
        # most non-select commands.
        columns = result_proxy.keys()

        # If no columns are mentioned in the cursor description
        # then there's no more data to return
        if not columns:
            return []

        # Pull all results out of the result proxy
        rows = list(result_proxy)

        # Put the data into tabular format
        table = dbreak.outputs.TableOutput(
            rows=rows,
            columns=columns
        )

        # Return the list of outputs
        return [table]

    @classmethod
    def handles(cls, raw_connection: object) -> bool:
        """ Returns True if this class can wrap the given connection

        :param raw_connection: An unwrapped database connection to test
        """

        # Must be an object of type Engine
        return type(raw_connection) is sqlalchemy.engine.Engine
