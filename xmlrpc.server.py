from xmlrpc.server import SimpleXMLRPCServer
import mysql.connector
import datetime


class Facade:

    # Connection configuration method
    def __init__(self, user, password, host, port=3306):
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

    # Creating connection with db
    def connect(self, database=None):
        config = self.config

        if database is not None:
            config.update({'database': database})

            # Defining connection object,
            # variable number of keyword arguments as a dictionary
            self.connection = mysql.connector.connect(**config)

    def execute_query(self, query, database=None, cursor_config={}):
        self.connect(database)

        self.cursor = self.connection.cursor(**cursor_config)

        self.cursor.execute(query)
        self.connection.commit()
        self.cursor.close()

    # Fetch records method
    def fetch_records(self, query, database, cursor_config={}):
        cursor_config.update({'buffered': True})

        self.execute_query(query, database, cursor_config)
        result = self.cursor.fetchall()

        self.cursor.close()

        return result


class Generate:

    def __init__(self, facade_storage: Facade, database: str):
        self.facade_storage = facade_storage
        self.database = database

    # task 1.
    def all_active_users(self):

        query = """
        SELECT 
        u.user_id,
        CASE
            WHEN s.sub_end_date >= NOW() THEN 'has sub.'
            ELSE 'no sub.'
        END AS sub_possession,
        ur.city,
        ur.country,
        s.sub_end_date,
        CASE
          WHEN s.sub_start_date > NOW() AND s.sub_end_date > NOW() THEN 'To Be Active'
          WHEN NOW() >= s.sub_start_date AND NOW() <= s.sub_end_date THEN 'Active'
          WHEN s.sub_end_date < NOW() THEN 'Inactive'
        END AS sub_status,
        d.device_type
        
        FROM subscription s
        
        RIGHT JOIN user u ON s.user_id = u.user_id
        INNER JOIN user_resident ur ON u.user_id = ur.user_id
        LEFT JOIN user_device ud ON s.user_id = ud.user_id
        LEFT JOIN device d ON ud.device_type = d.device_id
        
        WHERE NOW() >= s.sub_start_date AND NOW() <= s.sub_end_date
        
        ORDER BY u.user_id;
        """

        # Fetching query result
        list_dicts = self.facade_storage.fetch_records(
            query, self.database, cursor_config={"dictionary": True})

        # every second (even) record
        every_even_record = []

        for index, user_dict in enumerate(list_dicts, 1):
            if index % 2 == 0:
                every_even_record.append(user_dict)
                for key, val in user_dict.items():
                    # converting 'datetime.date' object into ISO 8601 format
                    if isinstance(val, datetime.date):
                        user_dict[key] = val.strftime("%d-%m-%Y")
                    else:
                        user_dict[key] = val

        return every_even_record

    # task 2.
    def all_inactive_users(self):

        query = """
        SELECT 
        u.user_id,
        CASE
            WHEN s.sub_end_date >= NOW() THEN 'has sub.'
            ELSE 'no sub.'
        END AS sub_possession,
        ur.city,
        ur.country,
        s.sub_end_date,
        CASE
          WHEN s.sub_start_date > NOW() AND s.sub_end_date > NOW() THEN 'To Be Active'
          WHEN NOW() >= s.sub_start_date AND NOW() <= s.sub_end_date THEN 'Active'
          WHEN s.sub_end_date < NOW() THEN 'Inactive'
        END AS sub_status,
        d.device_type
        
        FROM subscription s
        
        RIGHT JOIN user u ON s.user_id = u.user_id
        INNER JOIN user_resident ur ON u.user_id = ur.user_id
        LEFT JOIN user_device ud ON s.user_id = ud.user_id
        LEFT JOIN device d ON ud.device_type = d.device_id
        
        WHERE s.sub_end_date < NOW()
        
        ORDER BY u.user_id;
        """

        # Fetching query result
        list_dicts = self.facade_storage.fetch_records(
            query, self.database, cursor_config={"dictionary": True})

        # every second (even) record
        every_even_record = []

        for index, user_dict in enumerate(list_dicts, 1):
            if index % 2 == 0:
                every_even_record.append(user_dict)
                for key, val in user_dict.items():
                    # converting 'datetime.date' object into ISO 8601 format
                    if isinstance(val, datetime.date):
                        user_dict[key] = val.strftime("%d-%m-%Y")
                    else:
                        user_dict[key] = val

        return every_even_record

    # task 3.
    def sub_30d_end_austria_box(self):

        query = """
        SELECT 
        u.user_id,
        CASE
            WHEN s.sub_end_date >= NOW() THEN 'has sub.'
            ELSE 'no sub.'
        END AS sub_possession,
        ur.city,
        ur.country,
        s.sub_end_date,
        CASE
          WHEN s.sub_start_date > NOW() AND s.sub_end_date > NOW() THEN 'To Be Active'
          WHEN NOW() >= s.sub_start_date AND NOW() <= s.sub_end_date THEN 'Active'
          WHEN s.sub_end_date < NOW() THEN 'Inactive'
        END AS sub_status,
        d.device_type
        
        FROM subscription s
        
        RIGHT JOIN user u ON s.user_id = u.user_id
        INNER JOIN user_resident ur ON u.user_id = ur.user_id
        LEFT JOIN user_device ud ON s.user_id = ud.user_id
        LEFT JOIN device d ON ud.device_type = d.device_id
        
        WHERE ur.country = 'Austria'
        AND s.sub_end_date > DATE(NOW() + INTERVAL 28 DAY) + INTERVAL 0 SECOND
        AND s.sub_end_date < DATE(NOW() + INTERVAL 32 DAY) + INTERVAL 0 SECOND
        AND d.device_type = 'box'
        
        ORDER BY u.user_id;
        """

        # Fetching query result
        list_dicts = self.facade_storage.fetch_records(
            query, self.database, cursor_config={"dictionary": True})

        # every second (even) record
        every_even_record = []

        for index, user_dict in enumerate(list_dicts, 1):
            if index % 2 == 0:
                every_even_record.append(user_dict)
                for key, val in user_dict.items():
                    # converting 'datetime.date' object into ISO 8601 format
                    if isinstance(val, datetime.date):
                        user_dict[key] = val.strftime("%d-%m-%Y")
                    else:
                        user_dict[key] = val

        return every_even_record

    # task 4.
    def all_users(self):

        query = """
        SELECT 
        u.user_id,
        CASE
            WHEN s.sub_end_date >= NOW() THEN 'has sub.'
            ELSE 'no sub.'
        END AS sub_possession,
        ur.city,
        ur.country,
        s.sub_end_date,
        CASE
          WHEN s.sub_start_date > NOW() AND s.sub_end_date > NOW() THEN 'To Be Active'
          WHEN NOW() >= s.sub_start_date AND NOW() <= s.sub_end_date THEN 'Active'
          WHEN s.sub_end_date < NOW() THEN 'Inactive'
        END AS sub_status,
        d.device_type
        
        FROM subscription s
        
        RIGHT JOIN user u ON s.user_id = u.user_id
        INNER JOIN user_resident ur ON u.user_id = ur.user_id
        LEFT JOIN user_device ud ON s.user_id = ud.user_id
        LEFT JOIN device d ON ud.device_type = d.device_id
        
        ORDER BY u.user_id;
        """

        # Fetching query result
        list_dicts = self.facade_storage.fetch_records(
            query, self.database, cursor_config={"dictionary": True})

        # every second (even) record
        every_even_record = []

        for index, user_dict in enumerate(list_dicts, 1):
            if index % 2 == 0:
                every_even_record.append(user_dict)
                for key, val in user_dict.items():
                    # converting 'datetime.date' object into ISO 8601 format
                    if isinstance(val, datetime.date):
                        user_dict[key] = val.strftime("%d-%m-%Y")
                    else:
                        user_dict[key] = val

        return every_even_record


if __name__ == '__main__':

    # Creating and defining server:
    server = SimpleXMLRPCServer(('localhost', 18000), logRequests=True, allow_none=True)

    # Select database:
    database = 'database_name'

    # Inserting arguments in class object:
    facade_storage = Facade('root', 'user_password', 'localhost')

    # Instantiating class:
    generative_class = Generate(facade_storage, database)

    # Registration of Classes:
    server.register_instance(facade_storage)
    server.register_instance(generative_class)

    # Run Server:
    try:
        print('Serving on port 18000...')
        server.serve_forever()

    except KeyboardInterrupt:
        print('Exiting')

