from DbConnector import DbConnector
import os
import tqdm

class DBManager:
    
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db
    
    def create_coll(self, collection_name):
        collection = self.db.create_collection(collection_name)    
        print('Created collection: ', collection)
    
    def show_coll(self):
        collections = self.db.list_collection_names()
        print(collections)

    def drop_coll(self, collection_name):
        collection = self.db[collection_name]
        collection.drop()
    
    def drop_colls(self, coll_names):
        for name in coll_names:
            self.drop_coll(name)

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
            #if label_dict:
                #self.insert_user(user_id, True)
            #else:
             #   pass
                #self.insert_user(user_id, False)

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
                        #activity_id = self.insert_activity(user_id)

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
                                #self.update_activity(activity_id, transportation_mode, start, end)

                                #activity_id = self.insert_activity(user_id)
                                start, end = "", ""

                        #self.batch_insert_trackpoints(trackpoints)


def main():
    db = None
    try:
        db = DBManager()

        # Dropping collumns 
        db.drop_colls(['User', 'Activity', 'TrackPoint'])

        # Adding collections
        db.create_coll(collection_name="User")
        db.create_coll(collection_name="Activity")
        db.create_coll(collection_name="TrackPoint")
        db.show_coll()

        

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if db:
            db.connection.close_connection()


if __name__ == '__main__':
    main()