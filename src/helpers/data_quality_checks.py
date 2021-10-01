import configparser
import psycopg2


class DataQualityChecks:
    

    @staticmethod
    def custom_check(cur, conn, table_name, query, expected_result):
        """
        Asserts that results from a query match an expected result passed in by the user.

        Parameters
        ----------
            cur (Cursor) - cursor object from psycopg2 to execute query
            conn (Connection) - database connection object from psycopg2
            query (str) - the query to validate results against
            expected_result (int) - result expected from provided query
        """
        cur.execute(query)
        results = cur.fetchone()

        assert results[0] == expected_result


    @staticmethod
    def has_rows(cur, conn, table_name):
        """
        Asserts that table has at least one row.   
        
        Parameters
        ----------
            cur (Cursor) - cursor object from psycopg2 to execute query
            conn (Connection) - database connection object from psycopg2
            table_name (str) - the table in question
        """
        query = "SELECT COUNT(*) FROM {};".format(table_name)
        cur.execute(query)
        results = cur.fetchone()
        row_count = results[0]

        assert row_count > 0
        

    @staticmethod
    def row_count_between(cur, conn, table_name, lower_bound, upper_bound):
        """
        Asserts that table has at least one row.   
        
        Parameters
        ----------
            cur (Cursor) - cursor object from psycopg2 to execute query
            conn (Connection) - database connection object from psycopg2
            table_name (str) - the table in question
            lower_bound (int) - smallest number of rows table should have
            upper_bound (int) - largest number of rows table should have
        """
        query = 'SELECT COUNT (*) FROM {};'.format(table_name)
        cur.execute(query)
        results = cur.fetchone()
        row_count = results[0]

        assert lower_bound <= row_count <= upper_bound


    @staticmethod
    def no_nulls(cur, conn, table_name, column_name):
        """
        Asserts that a column does not contain any NULL values.
        
        Parameters
        ----------
            cur (Cursor) - cursor object from psycopg2 to execute query
            conn (Connection) - database connection object from psycopg2
            table_name (str) - the table containing the column in question
            column_name (str) - the column to check
        """

        query = 'SELECT COUNT (*) FROM {} WHERE {} IS NULL;'.format(table_name, column_name)
        cur.execute(query)
        results = cur.fetchone()
        row_count = results[0]

        assert row_count == 0


    @staticmethod
    def all_distinct(cur, conn, table_name, column_name):
        """
        Asserts that all values in a particular column of a table are distinct.

        Parameters
        ----------
            cur (Cursor) - cursor object from psycopg2 to execute query
            conn (Connection) - database connection object from psycopg2
            table_name (str) - the table containing the column in question
            column_name (str) - the column to check
        """

        expectation_query = "SELECT COUNT({}) FROM {};".format(column_name, table_name)
        cur.execute(expectation_query)
        expected_result = cur.fetchone()[0]

        actual_query = "SELECT COUNT(DISTINCT {}) FROM {};".format(column_name, table_name)
        cur.execute(actual_query)
        actual_result = cur.fetchone()[0]

        assert expected_result == actual_result



    mapping_dict = {
        'custom_check': custom_check.__func__,
        'has_rows': has_rows.__func__,
        'row_count_between': row_count_between.__func__,
        'no_nulls': no_nulls.__func__,
        'all_distinct': all_distinct.__func__,
    }



