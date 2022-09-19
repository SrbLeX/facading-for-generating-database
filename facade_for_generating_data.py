import mysql.connector
from mysql.connector import Error
import xlsxwriter
import datetime


class Facade:

    # connect configuration constructor method
    def __init__(self, user, password, host, port=3306):
        # Defining connection(args) in a dictionary
        self.config = {
            'user': user,
            'password': password,
            'host': host,
            'port': port,
        }

        # connection object
        self.connection = None

        # cursor object
        self.cursor = None

    # creating db connection method
    def connect(self, database=None):
        # instantiating 'self.config'(constructor method) into 'config' obj.
        config = self.config

        if database is not None:
            config.update({'database': database})

        try:
            # defining connection object,
            # variable number of keyword arguments as a dictionary
            self.connection = mysql.connector.connect(**config)
            print("MySQL DataBase connection successful")

        except Error as err:
            print(f"Error: '{err}'")

    def execute_query(self, query, database=None, cursor_config={}):
        # updating connect method with database
        self.connect(database)

        # defining cursor object
        self.cursor = self.connection.cursor(**cursor_config)

        try:
            self.cursor.execute(query)
            self.connection.commit()
            self.cursor.close()
            print("Query successful")
            print()

        except Error as err:
            print(f"Error: '{err}'")
            print()

    def execute_many(self, stmt, data, database=None):
        self.connect(database)

        self.cursor = self.connection.cursor()

        try:
            self.cursor.executemany(stmt, data)
            self.connection.commit()
            self.cursor.close()
            print("Query successful")
            print()

        except Error as err:
            print(f"Error: '{err}'")
            print()

    def create_database(self, database):
        try:
            print(f"Creating a DataBase '{database}'")
            self.execute_query(f"CREATE DATABASE {database}")

        except Error as err:
            print(f"Error: '{err}'")

    # fetch method
    def fetch_records(self, query, database, cursor_config={}):
        try:
            cursor_config.update({'buffered': True})

            print("Fetching records")
            self.execute_query(query, database, cursor_config)
            result = self.cursor.fetchall()

            self.cursor.close()

        except Error as err:
            print(f"Failed to read data from table Error: '{err}'")
            print()

        return result

    # backup_db
    def dump_db(self):
        self.database = database

        backup_db = self.database + "_backup"

        print(f"Show Tables in the Database '{self.database}'")
        result = self.fetch_records("SHOW TABLES", self.database)

        table_names = [record[0] for record in result]
        print(f"List of tables in the Database '{self.database}': {table_names}")

        try:
            print(f"Creating a '{backup_db}' DataBase")
            self.execute_query(f"CREATE DATABASE {backup_db}")

        except Error as err:
            print(f"Error: '{err}'")
            print()

        for table_name in table_names:
            print(f"Droping a Table '{table_name}' if exists")
            self.execute_query(
                f"DROP TABLE IF EXISTS {table_name}",
                backup_db
            )
            print(f"Creating a Table '{table_name}' in '{backup_db}'")
            self.execute_query(
                f"CREATE TABLE {table_name} SELECT * FROM {self.database}.{table_name}",
                backup_db
            )


# Generating and populating db class
class GenerateDbTableData:

    # ' : ' Facade class is located in parent directory
    def __init__(self, facade_storage: Facade, database: str):
        self.facade_storage = facade_storage
        self.database = database

    def create_db_nttv(self):
        self.facade_storage.create_database(self.database)

    def generate_tbls_db_nttv(self):

        create_table_user = """
                            CREATE TABLE user 
                            (
                            user_id INT AUTO_INCREMENT NOT NULL,
                            first_name VARCHAR(35) NOT NULL,
                            last_name VARCHAR(35) NOT NULL,
                            birth_date DATE NOT NULL,
                            PRIMARY KEY (user_id)
                            );
                            """

        insert_val_user = """
                                INSERT INTO user
                            (
                                first_name,
                                last_name,
                                birth_date
                            )
                            VALUES
                            ( 'Vojislav', 'Karadzic', '1992-03-14' ),
                            ( 'Nikola','Jeftic','1988-04-21' ),
                            ( 'Dusan','Milic','1981-11-12' ),
                            ( 'Marko', 'Grujic', '1985-02-19' ),
                            ( 'Zika', 'Spremic', '1976-10-21' ),
                            ( 'Stevan', 'Grdic', '1999-08-24' ),
                            ('Luka', 'Sindjelic', '2002-05-18'),
                            ('Uros', 'Lazovic', '1990-12-01'),
                            ('Stefan', 'Petrovic', '1997-06-07'),
                            ('Zoran', 'Soskic', '1975-05-24'),
                            ('Svetomir', 'Ilic', '1996-06-15'),
                            ('Jaroslav', 'Murdinic', '1985-09-06'),
                            ('Lazar', 'Knezevic', '1990-09-17');
                            """

        create_table_user_resident = """
                                    CREATE TABLE user_resident
                                    (
                                    resident_id INT NOT NULL AUTO_INCREMENT,
                                    user_id INT, 
                                    country VARCHAR(30) NOT NULL,
                                    city VARCHAR(30) NOT NULL,
                                    address VARCHAR(30) NOT NULL,
                                    zip_code VARCHAR(10) NOT NULL,
                                    state VARCHAR(30) DEFAULT 'Not US',
                                    PRIMARY KEY (resident_id),
                                    FOREIGN KEY (user_id) REFERENCES user (user_id)
                                    );
                                    """

        insert_val_user_resident = """
                                    INSERT INTO user_resident
                                    (
                                    user_id,
                                    country,
                                    city,
                                    address,
                                    zip_code,
                                    state
                                    )
                                    VALUES
                                    (1, 'Germany', 'Dusseldorf', 'Charlottenstrasse 51', '40210', DEFAULT),
                                    (2, 'France', 'Dijon', 'Av. Gustave Eiffel', '21000', DEFAULT),
                                    (3, 'Switzerland', 'Zurich', 'Dennlerstrasse 11A', '8048', DEFAULT),
                                    (4, 'USA', 'Las Vegas', '6628 Coley Ave', '89146', 'NV'),
                                    (5, 'Sweden', 'Stockholm', 'Logvagen 30', '16355', DEFAULT),
                                    (6, 'Germany', 'Augsburg', 'Bunchenstrase 16', '86179', DEFAULT),
                                    (7, 'Austria', 'Wien', 'Otakringerstrase 33', '1160', DEFAULT),
                                    (8, 'Austria', 'Wien', 'Kirchenstrase 13', '1130', DEFAULT),
                                    (9, 'Austria', 'Innsbruk', 'Andechstrase 44', '6020', DEFAULT),
                                    (10, 'USA', 'Chicago', ' 1845 W 35th St', '60609', 'IL'),
                                    (11, 'Austria', 'Innsbruk', 'Klepangasse 19', '6010', DEFAULT),
                                    (12, 'Austria', 'Wien', 'Mariahilfestrasse 3', '1110', DEFAULT),
                                    (13, 'Austria', 'Graz', 'Friedmanngasse 15', '8041', DEFAULT);
                                    """

        create_table_user_email = """
                                    CREATE TABLE user_email
                                    (
                                    email_id INT NOT NULL AUTO_INCREMENT,
                                    email VARCHAR(60),
                                    user_id INT,
                                    Is_deleted BOOLEAN,
                                    PRIMARY KEY (email_id),
                                    FOREIGN KEY (user_id) REFERENCES user (user_id)
                                    );
                                    """

        insert_val_user_email = """
                                    INSERT INTO user_email
                                    (
                                    email,
                                    user_id,
                                    Is_deleted
                                    )
                                    VALUES
                                    ('marko.grujic@gmail.com', 4, false), 
                                    ('nikola.jeftic@gmail.com', 2, false),
                                    ('vojislav.karadzic@yahoo.com', 1, false),
                                    ('vojislav.karadzic@gmail.com', 1, false),
                                    ('zika.spremic@hotmail.com', 5, false),
                                    ('dusan.milic@yahoo.com', 3, false),
                                    ('stevan.grdic@gmail.com', 6, false),
                                    ('luka.sindjelic@hotmail.com', 7, false),
                                    ('uros.lazovic@gmail.com', 8 , false),
                                    ('stefan.petrovic@yahoot.com', 9, false),
                                    ('zoran.soskic@hotmail.com', 10, false),
                                    ('svetomir.ilic@gmail.com', 11, false),
                                    ('jaroslav.mudrinic@yahoo.com', 12, false),
                                    ('lazar.knezevic@gmail', 13, false);
                                    """

        create_table_user_phone = """
                                    CREATE TABLE user_phone
                                    (
                                    phone_id INT NOT NULL AUTO_INCREMENT,
                                    phone_number VARCHAR(30),
                                    user_id INT,
                                    Is_deleted BOOLEAN,
                                    PRIMARY KEY (phone_id),
                                    FOREIGN KEY (user_id) REFERENCES user (user_id)
                                    );
                                    """

        insert_val_user_user_phone = """
                                    INSERT INTO user_phone
                                    (
                                    phone_number,
                                    user_id,
                                    Is_deleted
                                    )
                                    VALUES
                                    ('+17022340230', 4, false),
                                    ('+33380449733', 2, false),
                                    ('+4921135540', 1, false),
                                    ('+4687227240', 5, false),
                                    ('+46736798060', 5, false),
                                    ('+41433004500', 3, false),
                                    ('+41438115181', 3, false),
                                    ('+41417288090', 3, false),
                                    ('+4917180057', 6, false),
                                    ('+4318903165', 7, false),
                                    ('+4318902450', 8, false),
                                    ('+4312343160', 9, false),
                                    ('+13125774655', 10, false),
                                    ('+4312344516', 11, false),
                                    ('+4313343160', 12, false),
                                    ('+4322343151', 13, false);
                                    """

        create_table_package = """
                                CREATE TABLE package
                                (
                                package_id INT NOT NULL AUTO_INCREMENT,
                                package_type VARCHAR(20) NOT NULL UNIQUE,
                                PRIMARY KEY (package_id)
                                );
                                """

        insert_val_package = """
                                INSERT INTO package
                                (
                                package_type
                                )
                                VALUES
                                ('basic'),
                                ('standard'),
                                ('premium');
                                """

        create_table_device = """
                                CREATE TABLE device
                                (
                                device_id INT NOT NULL AUTO_INCREMENT,
                                device_type VARCHAR(20) UNIQUE,
                                PRIMARY KEY (device_id)
                                );
                                """

        insert_val_device = """
                                 INSERT INTO device
                                (
                                device_type
                                )
                                VALUES
                                ('box'),
                                ('smart tv'),
                                ('pc'),
                                ('mob app');
                                """

        create_table_subscription = """
                                     CREATE TABLE subscription
                                    (
                                    sub_id INT NOT NULL AUTO_INCREMENT,
                                    package_type INT, 
                                    user_id INT,
                                    sub_start_date DATETIME,
                                    sub_end_date DATETIME,
                                    PRIMARY KEY (sub_id),
                                    FOREIGN KEY (package_type) REFERENCES package (package_id),
                                    FOREIGN KEY (user_id) REFERENCES user (user_id)
                                    );
                                    """

        insert_val_subscription = """
                                    INSERT INTO subscription
                                    (
                                    package_type,
                                    user_id,
                                    sub_start_date,
                                    sub_end_date
                                    )
                                    VALUES
                                    (1, 1, '2020-05-20 00:00:01', '2022-05-20 23:59:59'),
                                    (2, 1, '2022-05-20 00:00:01', '2024-05-20 23:59:59'),
                                    (3, 2, '2023-05-20 00:00:01', '2025-05-20 23:59:59'),
                                    (1, 3, '2022-05-20 00:00:01', '2024-05-20 23:59:59'),
                                    (3, 4, '2021-05-20 00:00:01', '2023-05-20 23:59:59'),
                                    (2, 5, '2020-03-13 00:00:01', '2022-03-13 23:59:59'),
                                    (2, 5, '2022-09-17 00:00:01', '2024-09-17 23:59:59'),
                                    (1, 6, '2021-07-13 00:00:01', '2023-07-13 23:59:59'),
                                    (1, 7, '2020-10-06 00:00:01', '2022-10-06 23:59:59'),
                                    (1, 8, '2020-08-31 00:00:01', '2022-08-31 23:59:59'),
                                    (1, 11,'2020-10-06 00:00:01', '2022-10-06 23:59:59'),
                                    (2, 12, '2020-10-05 00:00:01', '2022-10-06 23:59:59'),
                                    (1, 13, '2020-10-03 00:00:01', '2022-10-03 23:59:59');
                                    """

        create_table_user_device = """
                                    CREATE TABLE user_device
                                    (
                                    user_device_id INT NOT NULL AUTO_INCREMENT,
                                    device_type INT,
                                    user_id INT,
                                    PRIMARY KEY (user_device_id),
                                    FOREIGN KEY (device_type) REFERENCES device (device_id),
                                    FOREIGN KEY (user_id) REFERENCES user (user_id)
                                    );
                                    """

        insert_val_user_device = """
                                    INSERT INTO user_device
                                    (
                                    device_type,
                                    user_id
                                    )
                                    VALUES
                                    (2, 1), (4, 1), (3, 1),
                                    (1, 2), (2, 2), (2, 2), (3, 2), (4, 2), (4, 2),
                                    (1, 3), (3, 3), (4, 3),
                                    (1, 4), (3, 4),
                                    (2, 5), (2, 5), (4, 5),
                                    (2, 6), (3, 6), (3, 6),
                                    (1, 7),
                                    (1, 8),
                                    (1, 11),
                                    (1, 12), (3, 12), (4, 12),
                                    (1, 13);
                                    """

        print("Creating a table 'user'")
        self.facade_storage.execute_query(create_table_user, self.database)

        print("Inserting values in table 'user'")
        self.facade_storage.execute_query(insert_val_user, self.database)

        print("Creating a table 'user_resident'")
        self.facade_storage.execute_query(create_table_user_resident, self.database)

        print("Inserting values in table 'user_resident'")
        self.facade_storage.execute_query(insert_val_user_resident, self.database)

        print("Creating a table 'user_email'")
        self.facade_storage.execute_query(create_table_user_email, self.database)

        print("Inserting values in table 'user_email'")
        self.facade_storage.execute_query(insert_val_user_email, self.database)

        print("Creating a table 'user_phone'")
        self.facade_storage.execute_query(create_table_user_phone, self.database)

        print("Inserting values in table 'user_phone'")
        self.facade_storage.execute_query(insert_val_user_user_phone, self.database)

        print("Creating a table 'package'")
        self.facade_storage.execute_query(create_table_package, self.database)

        print("Inserting values in table 'package'")
        self.facade_storage.execute_query(insert_val_package, self.database)

        print("Creating a table 'device'")
        self.facade_storage.execute_query(create_table_device, self.database)

        print("Inserting values in table 'device'")
        self.facade_storage.execute_query(insert_val_device, self.database)

        print("Creating a table 'subscription'")
        self.facade_storage.execute_query(create_table_subscription, self.database)

        print("Inserting values in table 'subscription'")
        self.facade_storage.execute_query(insert_val_subscription, self.database)

        print("Creating a table 'user_device'")
        self.facade_storage.execute_query(create_table_user_device, self.database)

        print("Inserting values in table 'user_device'")
        self.facade_storage.execute_query(insert_val_user_device, self.database)

    # fetching records from req. query
    def requested_query_record(self, query):
        try:
            qurey_records = self.facade_storage.fetch_records(
                query, self.database, cursor_config={"dictionary": True}
            )

            if len(qurey_records) > 0:
                users = [user for user in qurey_records]
                print(f"Requested query records contains {len(users)} users")
                print()

        except Error as err:
            print(f"Error: '{err}'")
            print()

        return qurey_records

    def requested_query(self):

        query = """
                SELECT 
                u.user_id,
                u.first_name,
                u.last_name,
                ue.email,
                up.phone_number,
                ur.city,
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
                INNER JOIN device d ON d.device_id = ud.device_type
                
                WHERE ur.country = 'Germany' AND d.device_type = 'smart tv' AND s.sub_end_date > NOW()
                
                GROUP BY u.user_id
                ORDER BY s.sub_end_date;
                """

        return self.requested_query_record(query)


class ExportToExcel:

    def requested_query_export(self, file_name, sheet_name, data):

        # Create a workbook and add a worksheet
        workbook = xlsxwriter.Workbook(file_name + '.xlsx')
        worksheet = workbook.add_worksheet(sheet_name)

        # Define the width of the columns
        last = len(data[0].keys())
        column_width = worksheet.set_column(0, last, 25)

        # Define the format of column names
        header_cell_format = workbook.add_format(
            {'bold': True, 'border': True, 'bg_color': 'yellow'}
        )

        # Defining and formatting record columns
        body_cell_format = workbook.add_format({'border': True})

        # Defining and formatting record columns and type(datetime.date into 'dd/mm/yyyy')
        body_date_cell_format = workbook.add_format(
            {'num_format': 'dd/mm/yyyy', 'border': True}
        )

        # Writing "header"- column names on Excel sheet
        row_index = 0
        for column_index, column_name in enumerate(data[0].keys()):
            worksheet.write(row_index, column_index, column_name, column_width)
            worksheet.write(row_index, column_index, column_name, header_cell_format)

        # Printing records on Excel sheet
        for row_index, row in enumerate(data, 1):
            for column_index, column in enumerate(row.values()):
                if isinstance(column, datetime.date):
                    worksheet.write(row_index, column_index, column, body_date_cell_format)
                else:
                    worksheet.write(row_index, column_index, column, body_cell_format)

        print(f"{len(data)} rows written successfully to {workbook.filename}, {sheet_name}")
        print()

        # Closing workbook
        workbook.close()


if __name__ == "__main__":
    # Select database (name):
    database = 'db_name'

    # instantiating and defining Facade class and its parameters
    facade_storage = Facade('user', 'user_pass.', 'user_host')

    # instantiating classes:
    generative_class = GenerateDbTableData(facade_storage, database)
    export_to_excel = ExportToExcel()

    # calling functions:
    generative_class.create_db_nttv()
    generative_class.generate_tbls_db_nttv()
    # dump_db() function (attribute of facade_storage obj.) needs to be called
    # separately, while the previous two functions are commented:

    # facade_storage.dump_db()

    # calling export to excel function:
    data_for_excel = generative_class.requested_query()
    export_to_excel.requested_query_export('exercise2', 'sheet2', data_for_excel)
