import mysql.connector
from mysql.connector import Error
import xlsxwriter
import datetime


class Facade:

    # cnx configuration constructor method
    def __init__(self, user, host, password, port=3306):
        self.config = {
            'user': user,
            'host': host,
            'password': password,
            'port': port
        }

        self.cnx = None
        self.cursor = None

    # creating db connection method
    def connect(self, database=None):
        # instantiating 'self.config'(constructor method) into 'config' obj.
        config = self.config

        if database not in config:
            config.update({'database': database})

        try:
            # defining connection object,
            # variable number of keyword arguments as a dictionary
            self.cnx = mysql.connector.connect(**config)
            print("MySQL DataBase connection successful")

        except Error as err:
            print(f"Error: '{err}'")

    def execute_query(self, query, database=None):
        # updating connect method with database
        self.connect(database)

        # defining and buffering cursor object
        self.cursor = self.cnx.cursor(buffered=True)

        try:
            self.cursor.execute(query)
            self.cnx.commit()
            self.cursor.close()
            print("Query successful")

        except Error as err:
            print(f"Error: '{err}'")

    # fetch method
    def fetch_records(self, query, database):
        try:
            print("\nFetching records")
            self.execute_query(query, database)
            result = self.cursor.fetchall()

            self.cursor.close()

        except Error as err:
            print(f"Failed to read data from table Error: '{err}'")

        return result

    # backup_db
    def dump_db(self):
        backup_db = database + "_dump"
        self.execute_query(f"CREATE DATABASE {backup_db}")

        table_names = self.fetch_records("SHOW TABLES", database)
        for table_name in table_names:
            self.execute_query(
                f"CREATE TABLE {table_name[0]} SELECT * FROM {database}.{table_name[0]}", backup_db
            )


# Generating and populating db class
class GenerateDbTableData(Facade):

    def create_db(self):
        self.execute_query(f"CREATE DATABASE {database}")

    def show_tables(self):
        result = self.fetch_records("SHOW TABLES", database)

        return result

    def table_header(self, query):
        self.execute_query(query, database)
        result = [column[0] for column in self.cursor.description]

        return result

    def records(self, query):
        result = self.fetch_records(query, database)
        if len(result) > 0:
            print(f"\nRequested query has {len(result)} users")

        # converting 'datetime.date' object into ISO 8601 format
        converted_date = []
        for user in result:
            converted_date.append([])
            for column in user:
                if isinstance(column, datetime.date):
                    converted_date[-1].append(column.strftime("%d-%m-%Y"))
                else:
                    converted_date[-1].append(column)

        return converted_date

    def req_query_header(self):
        query = """
                SELECT
                s.user_id,
                u.first_name,
                u.last_name,
                ue.email,
                up.phone_number,
                ur.country,
                CASE
                    WHEN NOW() >= s.sub_start_date AND NOW() <= s.sub_end_date THEN 'Active'
                    WHEN s.sub_end_date < NOW() THEN 'Inactive'
                    WHEN s.sub_start_date > NOW() AND s.sub_end_date > NOW() THEN 'To Be Active'
                END AS  sub_status,
                s.sub_end_date,
                d.device_type
    
                FROM subscription s
    
                LEFT JOIN user u ON s.user_id = u.user_id
                LEFT JOIN user_email ue ON s.user_id = ue.user_id
                LEFT JOIN user_phone up ON s.user_id = up.user_id
                LEFT JOIN user_resident ur ON s.user_id = ur.user_id
                LEFT JOIN user_device ud ON s.user_id = ud.user_id
                INNER JOIN device d ON ud.device_type = d.device_id
    
                WHERE NOW() >= s.sub_start_date AND NOW() <= s.sub_end_date
                AND ur.country = 'Germany'
                AND d.device_type = 'smart tv'
                GROUP BY s.user_id
                ORDER BY s.sub_end_date; 
                """

        column_names = self.table_header(query)
        print(f"\nThe names of the columns in the 'req_query_header' are: {column_names}")

        return column_names

    def req_query_records(self):
        query = """
                        SELECT
                        s.user_id,
                        u.first_name,
                        u.last_name,
                        ue.email,
                        up.phone_number,
                        ur.country,
                        CASE
                            WHEN NOW() >= s.sub_start_date AND NOW() <= s.sub_end_date THEN 'Active'
                            WHEN s.sub_end_date < NOW() THEN 'Inactive'
                            WHEN s.sub_start_date > NOW() AND s.sub_end_date > NOW() THEN 'To Be Active'
                        END AS  sub_status,
                        s.sub_start_date,
                        s.sub_end_date,
                        d.device_type

                        FROM subscription s

                        LEFT JOIN user u ON s.user_id = u.user_id
                        LEFT JOIN user_email ue ON s.user_id = ue.user_id
                        LEFT JOIN user_phone up ON s.user_id = up.user_id
                        LEFT JOIN user_resident ur ON s.user_id = ur.user_id
                        LEFT JOIN user_device ud ON s.user_id = ud.user_id
                        INNER JOIN device d ON ud.device_type = d.device_id

                        WHERE NOW() >= s.sub_start_date AND NOW() <= s.sub_end_date
                        AND ur.country = 'Germany'
                        AND d.device_type = 'smart tv'
                        GROUP BY s.user_id
                        ORDER BY s.sub_end_date; 
                        """

        records = self.records(query)
        print(f"\nThe content of the records in the 'req_query_records' is: {records}")

        return records


class ExpToExcel:

    def req_query_excel(self, filename, sheet_name, column_names, records):
        workbook = xlsxwriter.Workbook(filename + '.xlsx')
        worksheet = workbook.add_worksheet(sheet_name)

        row_index = 0
        for column_index, column_name in enumerate(column_names):
            worksheet.write(row_index, column_index, column_name)

        for row_index, row in enumerate(records, 1):
            for column_index, column in enumerate(row):
                worksheet.write(row_index, column_index, column)

        workbook.close()


if __name__ == "__main__":
    generate = GenerateDbTableData('user', 'user_host', 'user_pass.')
    database = 'db_name'
    exp_to_excel = ExpToExcel()

    # generate.create_db()
    # generate.show_tables()
    # generate.dump_db()
    # generate.req_query_records()
    data_header = generate.req_query_header()
    data_records = generate.req_query_records()
    exp_to_excel.req_query_excel('exercise', 'sheet1', data_header, data_records)
