import time
from DbConnector import DbConnector
from tabulate import tabulate
import os
from tqdm import tqdm


class DBManager:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_table(self, table_name, columns_definition):
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_definition})"
        
        self.cursor.execute(query)
        self.db_connection.commit()
        print("Table", table_name, "created!")


    def insert_user(self, user_id, has_labels):  
        insert_query = """
            INSERT INTO User (id, has_labels)
            VALUES (%s, %s)
        """

        self.cursor.execute(insert_query, (user_id, has_labels))
        self.db_connection.commit()
    
    def insert_activity(self, user_id):
        insert_query = """
            INSERT INTO Activity (user_id)
            VALUES (%s)
        """

        self.cursor.execute(insert_query, (user_id,))
        self.db_connection.commit()
        activity_id = self.cursor.lastrowid

        return activity_id
    
    def update_activity(db_setup, activity_id, transportation_mode, start_time, end_time):
        update_query = """
            UPDATE Activity
            SET transportation_mode = %s,
                start_date_time = %s,
                end_date_time = %s
            WHERE id = %s
        """

        db_setup.cursor.execute(update_query, (transportation_mode, start_time, end_time, activity_id))
        db_setup.db_connection.commit()
    
    def batch_insert_trackpoints(self, trackpoints):
        insert_query = """
            INSERT INTO TrackPoint (activity_id, lat, lon, altitude, date_days, date_time)
            VALUES (%s, %s, %s, %s, %s, %s)
        """

        self.cursor.executemany(insert_query, trackpoints)
        self.db_connection.commit()

    def drop_tables(self):
        tables_to_drop = ["TrackPoint", "Activity", "User"]
        for table in tables_to_drop:
            drop_query = f"DROP TABLE IF EXISTS {table}"
            self.cursor.execute(drop_query)
            print(f"Dropped table: {table}")

        self.db_connection.commit()
        print("All tables dropped successfully.")


    def show_tables(self):
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))
        
    def show_table(self, table_name, limit=None):
    
        if limit is not None and isinstance(limit, int):
            query = f"SELECT * FROM {table_name} LIMIT {limit}"
        else:
            query = f"SELECT * FROM {table_name}"

        self.cursor.execute(query)
        
        rows = self.cursor.fetchall()
        
        if rows:
            print(tabulate(rows, headers=self.cursor.column_names, tablefmt="pretty"))
        else:
            print(f"The table '{table_name}' is empty or does not exist.")


    def execute_query(self, query, params=None):
        if params:
            self.cursor.execute(query, params)
        else: 
            self.cursor.execute(query)

        # If it's a SELECT statement, fetch and return the results
        if query.strip().lower().startswith("select"):
            rows = self.cursor.fetchall()
            if rows:
                print(tabulate(rows, headers=self.cursor.column_names, tablefmt="pretty"))
            return rows
        else:
            self.db_connection.commit()
            print("Query executed successfully.")
            return None


    def execute_query_limited(self, query, params=None, limit=None, printOut=True):
        if params:
            self.cursor.execute(query, params)
        else:
            self.cursor.execute(query)

        # If it's a SELECT statement, fetch and return the results
        if query.strip().lower().startswith("select"):
            rows = self.cursor.fetchall()
            if rows and limit:
                limited_rows = rows[:limit]  # Limit the number of rows to print
                if printOut:
                    print(tabulate(limited_rows, headers=self.cursor.column_names, tablefmt="pretty"))
            return rows
        else:
            self.db_connection.commit()
            if printOut:
                print("Query executed successfully.")
            return None

    # Gets all files under a certain user
    def get_user_trajectories(self, user_folder_path):
        if os.path.isdir(user_folder_path):
            trajectory_folder_path = os.path.join(user_folder_path, 'Trajectory')
            
            if os.path.exists(trajectory_folder_path):
                return os.listdir(trajectory_folder_path), trajectory_folder_path

    # Gets all labels if they exist. Returns a dictionary with (start, end) as key and transportation mode as value.
    def get_user_labels(self, user_folder_path):
        labels_dict = {}

        if os.path.isdir(user_folder_path):
            labels_path = os.path.join(user_folder_path, 'labels.txt')

            if os.path.exists(labels_path):
                with open(labels_path, 'r') as file:
                    lines = file.readlines()

                    # Skip the header line and process the rest
                    for line in lines[1:]:
                        parts = line.strip().split('\t')
                        if len(parts) == 3:
                            start_time, end_time, transportation_mode = parts

                            # Ensuring right format on the date.
                            start_time = start_time.replace('/', '-')
                            end_time = end_time.replace('/', '-')

                            labels_dict[(start_time, end_time)] = transportation_mode

        return labels_dict

    # Splits a line in a plt_file into python variables (4 variables)
    def split_data_line(self, line):
        parts = line.strip().split(',')

        latitude = float(parts[0])             # Field 1: Latitude in decimal degrees
        longitude = float(parts[1])            # Field 2: Longitude in decimal degrees
                                               # Field 3 is set to 0 for the entire dataset, we can ignore it
        altitude = int(float(parts[3]))        # Field 4: Altitude in feet (-777 if not valid)
        date_days = float(parts[4])            # Field 5: Number of days since 12/30/1899
        date_str = parts[5].strip()            # Field 6: Date as a string (e.g., "2009-10-11")
        time_str = parts[6].strip()            # Field 7: Time as a string (e.g., "14:04:30")

        # We only care about the timestamp
        date_time = f"{date_str} {time_str}"
        
        return latitude, longitude, altitude, date_days, date_time

    def fill_database(self):
        data_folder_path = os.path.join("../dataset/dataset/Data")
        for user_id in tqdm(os.listdir(data_folder_path), desc="Filling database"):
            if user_id == ".DS_Store":
                continue

            user_folder_path = os.path.join(data_folder_path, user_id)

            # Check if labels.txt file exists
            label_dict = self.get_user_labels(user_folder_path)

            # INSERT DATA INTO USER
            if label_dict:
                self.insert_user(user_id, True)
            else:
                self.insert_user(user_id, False)

            # Go through trajectory files of a user
            plt_files, trajectory_folder_path = self.get_user_trajectories(user_folder_path)

            for plt_file in plt_files:
                start, end = "", ""

                if plt_file.endswith(".plt"):
                    plt_file_path = os.path.join(trajectory_folder_path, plt_file)

                    with open(plt_file_path, 'r') as file:
                        lines = file.readlines()
                        data_lines = lines[6:]

                        if len(data_lines) > 2500:
                            # SKIP ACTIVITY
                            continue

                        # INSERT DATA INTO ACTIVITY
                        activity_id = self.insert_activity(user_id)

                        trackpoints = []

                        for i in range(len(data_lines)):
                            lat, lon, altitude, date_days, date_time = self.split_data_line(data_lines[i])

                            if not start:
                                start = date_time

                            end = date_time

                            if altitude <= -777:
                                trackpoints.append((activity_id, lat, lon, None, date_days, date_time))
                            else:
                                trackpoints.append((activity_id, lat, lon, altitude, date_days, date_time))

                            if (start, end) in label_dict:
                                transportation_mode = label_dict[(start, end)]
                                self.update_activity(activity_id, transportation_mode, start, end)

                                activity_id = self.insert_activity(user_id)
                                start, end = "", ""

                        self.batch_insert_trackpoints(trackpoints)


# ASSUMES THAT THE DATASET IS IN THIS PROJECT FILE, I.E. SAME LEVEL.
def main():
    db = None
    try:
        db = DBManager()

        db.drop_tables()

        # CREATING THE TABLES FOR USER, ACTIVITY AND TRACKPOINT

        db.create_table(
            table_name="User",
            columns_definition="""
                id VARCHAR(50) NOT NULL PRIMARY KEY,
                has_labels BOOLEAN NOT NULL
            """
        )

        db.create_table(
            table_name="Activity",
            columns_definition="""
                id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                user_id VARCHAR(50) NOT NULL,
                transportation_mode VARCHAR(50),
                start_date_time DATETIME,
                end_date_time DATETIME,
                FOREIGN KEY (user_id) REFERENCES User(id)
            """
        )

        db.create_table(
            table_name="TrackPoint",
            columns_definition="""
                id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                activity_id INT NOT NULL,
                lat DOUBLE,
                lon DOUBLE,
                altitude INT,
                date_days DOUBLE,
                date_time DATETIME,
                FOREIGN KEY (activity_id) REFERENCES Activity(id)
            """
        )

        # FILLING THE DATABASE
        db.fill_database()
        
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if db:
            db.connection.close_connection()


if __name__ == '__main__':
    main()

    
    
    

    