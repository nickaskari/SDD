from DbConnector import DbConnector
import os
from tqdm import tqdm
from tabulate import tabulate


def flatten_data(document):
    flattened_doc = {}
    for key, value in document.items():
        # show the count instead of the whole list, i.ie. the number of activities or trackpoints
        if isinstance(value, list):
            flattened_doc[key] = f"List({len(value)})"  
        else:
            flattened_doc[key] = value  # For other data types, keep it as is
    
    return flattened_doc


class DBManager:
    
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db
        self.activity_counter = 1
        self.trackpoint_counter = 1
    
    def drop_coll(self, collection_name):
        collection = self.db[collection_name]
        collection.drop()
    
    def drop_colls(self, coll_names):
        for name in coll_names:
            self.drop_coll(name)

    def show_collections(self):
        collections = self.db.list_collection_names()
        print("Collections in the database:")
        for coll in collections:
            print(f" {coll}")


    def show_documents(self, collection_name, limit=None, sort_by=None, sort_order=1):
        """
        Display the documents in a specific collection in a tabular format.
        """
        if collection_name in self.db.list_collection_names():
            collection = self.db[collection_name]

            if sort_by:
                documents = collection.find().sort(sort_by, sort_order).limit(limit)
            else:
                documents = collection.find().limit(limit)

            documents_list = [flatten_data(doc) for doc in documents]  # Flatten

            if documents_list:
                print(f"Displaying up to {limit} documents from the '{collection_name}' collection:")
                print(tabulate(documents_list, headers="keys", tablefmt="pretty"))
            else:
                print(f"The '{collection_name}' collection is empty.")
        else:
            print(f"Collection '{collection_name}' does not exist.")

    
    def insert_trackpoints_batch(self, trackpoints):
        if trackpoints:
            self.db.TrackPoint.insert_many(trackpoints)
    
    def set_trackpoint_id(self, trackpoint):
        trackpoint["_id"] = self.trackpoint_counter
        self.trackpoint_counter += 1
        return trackpoint["_id"]

   
    def insert_activity(self, user_id, trackpoints, transportation_mode=None, start_time=None, end_time=None):
        activity_id = self.activity_counter  # custom human-readable ID
        activity_doc = {
            "_id": activity_id,  
            "user_id": user_id,
            "transportation_mode": transportation_mode,
            "start_date_time": start_time,
            "end_date_time": end_time,
            "trackpoints": trackpoints  # List of trackpoint ids (referenced by trackpoints)
        }
        result = self.db.Activity.insert_one(activity_doc)
        self.activity_counter += 1  
        return activity_id  

    def insert_user(self, user_id, activity_ids, has_labels):  
        user_doc = {
            "_id": user_id,
            "has_labels": has_labels,
            "activities": activity_ids  # Store the activity ids in the user document
        }

        try:
            self.db.User.update_one(
                {"_id": user_id}, 
                {"$set": user_doc}, 
                upsert=True
            )
        except Exception as e:
            print(f"Failed to insert/update user {user_id}: {e}")

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

        date_time = f"{date_str} {time_str}"
        
        return latitude, longitude, altitude, date_days, date_time

    

    def fill_database(self, batch_size=1000):
        data_folder_path = os.path.join("../dataset/dataset/Data")
        users = [user_id for user_id in os.listdir(data_folder_path) if user_id != ".DS_Store"]
        
        with tqdm(total=len(users), desc="Filling database", leave=True) as pbar:
            for user_id in users:
                user_folder_path = os.path.join(data_folder_path, user_id)
                label_dict = self.get_user_labels(user_folder_path)

                activities = []  
                trackpoints_batch = []  # Batch to store all trackpoints for bulk insertion

                plt_files, trajectory_folder_path = self.get_user_trajectories(user_folder_path)

                for plt_file in plt_files:
                    start, end = "", ""
                    trackpoint_ids = []  # for the current activity

                    if plt_file.endswith(".plt"):
                        plt_file_path = os.path.join(trajectory_folder_path, plt_file)

                        with open(plt_file_path, 'r') as file:
                            lines = file.readlines()
                            data_lines = lines[6:]

                            if len(data_lines) > 2500:
                                continue  

                            for i in range(len(data_lines)):
                                lat, lon, altitude, date_days, date_time = self.split_data_line(data_lines[i])

                                if not start:
                                    start = date_time
                                end = date_time
                 
                                trackpoint = {
                                    "lat": lat,
                                    "lon": lon,
                                    "altitude": altitude if altitude > -777 else None,
                                    "date_days": date_days,
                                    "date_time": date_time
                                }

                                trackpoint_id = self.set_trackpoint_id(trackpoint)

                                trackpoint_ids.append(trackpoint_id)
                                trackpoints_batch.append(trackpoint)

                                if (start, end) in label_dict:
                                    transportation_mode = label_dict[(start, end)]
                                    activity_id = self.insert_activity(user_id, trackpoint_ids, transportation_mode, start, end)
                                    activities.append(activity_id)
                                    trackpoint_ids = []  # Reset for the next activity
                                    start, end = "", ""

                            if trackpoint_ids:
                                activity_id = self.insert_activity(user_id, trackpoint_ids, None, start, end)
                                activities.append(activity_id)

                            if len(trackpoints_batch) >= batch_size:
                                self.insert_trackpoints_batch(trackpoints_batch)
                                trackpoints_batch = []  

                if trackpoints_batch:
                    self.insert_trackpoints_batch(trackpoints_batch)

                self.insert_user(user_id, activities, bool(label_dict))
                pbar.update(1) 
        

def main():
    db = None
    try:
        db = DBManager()

        # Dropping collections
        db.drop_colls(['User', 'Activity', 'TrackPoint'])

        db.fill_database()

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if db:
            db.connection.close_connection()


if __name__ == '__main__':
    main()
    
    '''
    db = DBManager()

    db.show_collections()

    db.show_documents('User', limit=11, sort_by="_id", sort_order=1)
    db.show_documents('Activity', limit=10)
    db.show_documents('TrackPoint', limit=5)
    '''
