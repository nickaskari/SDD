from DbConnector import DbConnector
from tabulate import tabulate


class DBSetup:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_table(self, table_name, columns_definition):
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_definition})"
        
        self.cursor.execute(query)
        self.db_connection.commit()


    def insert_data(self, table_name):
        names = ['Bobby', 'Mc', 'McSmack', 'Board']
        for name in names:
            # Take note that the name is wrapped in '' --> '%s' because it is a string,
            # while an int would be %s etc
            query = "INSERT INTO %s (name) VALUES ('%s')"
            self.cursor.execute(query % (table_name, name))
        self.db_connection.commit()

    def fetch_data(self, table_name):
        query = "SELECT * FROM %s"
        self.cursor.execute(query % table_name)
        rows = self.cursor.fetchall()
        print("Data from table %s, raw format:" % table_name)
        print(rows)
        # Using tabulate to show the table in a nice way
        print("Data from table %s, tabulated:" % table_name)
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def drop_table(self, table_name):
        print("Dropping table %s..." % table_name)
        query = "DROP TABLE %s"
        self.cursor.execute(query % table_name)

    def show_tables(self):
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))


def main():
    program = None
    try:
        program = DBSetup()

        # User table. has_labels can be Null (be default)
        program.create_table(
            table_name="User",
            columns_definition="""
                id VARCHAR(50) NOT NULL PRIMARY KEY,
                has_labels BOOLEAN NOT NULL
            """
        )

        # Creating the Activity table
        program.create_table(
            table_name="Activity",
            columns_definition="""
                id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                user_id VARCHAR(50) NOT NULL,
                transportation_mode VARCHAR(50),
                start_date_time DATETIME NOT NULL,
                end_date_time DATETIME NOT NULL,
                FOREIGN KEY (user_id) REFERENCES User(id)
            """
        )

        program.create_table(
            table_name="TrackPoint",
            columns_definition="""
                id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                activity_id INT NOT NULL,
                lat DOUBLE NOT NULL,
                lon DOUBLE NOT NULL,
                altitude INT,
                date_days DOUBLE,
                date_time DATETIME NOT NULL,
                FOREIGN KEY (activity_id) REFERENCES Activity(id)
            """
        )
   
        
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
