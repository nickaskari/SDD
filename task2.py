from DbConnector import DbConnector
from tabulate import tabulate
from tqdm import tqdm
from task1 import DBManager
from Haversine import haversine

class DBQueries:

    def __init__(self):
        self.db = DBManager()

    def query_1(self):
        ''' 
            Counts rows in the tables: User, Activity and TrackPoint. 
            Prints out the counts.
        '''
        tables = ["User", "Activity", "TrackPoint"]

        print("Number of rows for each of the tables:")
        for table_name in tables:
       
            count_query = f"SELECT COUNT(*) FROM {table_name}"

            print(table_name)
            self.db.execute_query(count_query)

    def query_2(self):
        ''' 
            Finds the average number of activities per user.
        '''

        print("Average number of activities per user:\n")
        avg_query = """
            SELECT AVG(activity_count)
            FROM (
                SELECT COUNT(*) AS activity_count
                FROM Activity
                GROUP BY user_id
            ) AS user_activity_counts
        """

        self.db.execute_query(avg_query)

    
    def query_3(self):
        ''' 
            Finds the top 20 users with the highest number of activities.
        '''

        print("Top 20 users with the highest number of activities:")
        top_users_query = """
            SELECT user_id, COUNT(*) AS activity_count
            FROM Activity
            GROUP BY user_id
            ORDER BY activity_count DESC
            LIMIT 20;
        """

        self.db.execute_query(top_users_query)


    def query_4(self):
        ''' 
            Finds all users who have taken a taxi
        '''

        print("All users who have taken a taxi:")
        taxi_users_query = """
            SELECT DISTINCT user_id
            FROM Activity
            WHERE transportation_mode = 'taxi';
        """

        self.db.execute_query(taxi_users_query)
    
    def query_5(self):
        ''' 
            Finds all types of transportation modes and count how many activities that are
            tagged with these transportation mode labels. Do not count the rows where
            the mode is null.
        '''

        print("Finds how many activities are tagged with each transportation mode label:")
        transportation_modes_query = """
            SELECT transportation_mode, COUNT(*) AS activity_count
            FROM Activity
            WHERE transportation_mode IS NOT NULL
            GROUP BY transportation_mode;
        """

        self.db.execute_query(transportation_modes_query)

    def query_6(self):
        ''' 
            a) Finds the year with the most activities.
            b) Answers whether the year found is also the year with the most recorded hours.
        '''

        # query a)
        print("The year with the most activities:")
        most_activities_year_query = """
            SELECT activity_year, COUNT(DISTINCT activity_id) AS activity_count
            FROM (
                SELECT activity_id, YEAR(date_time) AS activity_year
                FROM TrackPoint
            ) AS trackpoint_years
            GROUP BY activity_year
            ORDER BY activity_count DESC
            LIMIT 1;
        """

        result_a = self.db.execute_query(most_activities_year_query)

        if result_a:
            activity_year, activity_count = result_a[0]
            print(f"The year with the most activities is {activity_year} with {activity_count} activities.\n")

        # query b)
        print("The year with the most recorded hours (only showing first 5 activites):")
        most_hours_year_query = """
            SELECT activity_id, MIN(date_time) AS start_time, MAX(date_time) AS end_time
            FROM TrackPoint
            GROUP BY activity_id;
        """

        result_b = self.db.execute_query_limited(most_hours_year_query, None, 5)

        if result_b:
            year_hours = {}

            for _, start_time, end_time in result_b:
                duration_hours = (end_time - start_time).total_seconds() / 3600.0
                activity_year = start_time.year

                if activity_year in year_hours:
                    year_hours[activity_year] += duration_hours
                else:
                    year_hours[activity_year] = duration_hours

            sorted_year_hours = sorted(year_hours.items(), key=lambda x: x[1], reverse=True)

            table_data = [[year, f"{hours:.2f}"] for year, hours in sorted_year_hours]
            headers = ["Year", "Recorded Hours"]

            print(tabulate(table_data, headers=headers, tablefmt="pretty"))


    def query_7(self):
        ''' 
            Find the total distance (in km) walked in 2008, by user with id=112.
        '''

        print("All trackpoints walking in 2008 by user with id=112 (ordered by their activity ids, only showing first 5 entries):")
        trackpoints_query = """
            SELECT A.id, TP.lat, TP.lon
            FROM TrackPoint TP
            JOIN Activity A ON TP.activity_id = A.id
            WHERE A.user_id = 112 
                AND A.transportation_mode = 'walk' 
                AND YEAR(TP.date_time) = 2008
            ORDER BY A.id, TP.date_time;
        """

        result = self.db.execute_query_limited(trackpoints_query, None, 5)

        if result:
            total_distance_m = 0.0
        
            for i in range(1, len(result)):
                id1, lat1, lon1 = result[i - 1]
                id2, lat2, lon2 = result[i]

                if id1 != id2:
                    continue
            
                distance_m = haversine(lat1, lon1, lat2, lon2) # assuming a straight line
                total_distance_m += distance_m

            total_distance_km = total_distance_m / 1000.0

            print(f"Total distance walked in 2008 by user 112: {total_distance_km:.2f} km\n")

    def query_8(self):
        ''' 
            Find the top 10 users who have gained the most altitude meters.
        '''
        # Here we assume that all altitude values are valid, the cleaning is dealt with in task1.py

        print("Top 20 users who have gained the most altitude meters:")
        altitude_query = """
            SELECT A.user_id, 
                TP.altitude * 0.3048 AS altitude_meters, 
                TP.activity_id, 
                TP.date_time
            FROM TrackPoint TP
            JOIN Activity A ON TP.activity_id = A.id
            WHERE TP.altitude IS NOT NULL
            ORDER BY A.user_id, TP.activity_id, TP.date_time;
        """

        result = self.db.execute_query_limited(altitude_query, None, 10, printOut=False)

        if result:

            altitude_gain_per_user = {}

            prev_user = None
            prev_alt = None
            prev_activity = None

            for user, alt, activity, _ in result:
                
                if user != prev_user or activity != prev_activity:
                    prev_user = user
                    prev_activity = activity
                    prev_alt = None

                if prev_alt is not None:

                    alt_diff = alt - prev_alt
                    if alt_diff > 0.0:
                        if user in altitude_gain_per_user:
                            altitude_gain_per_user[user] += alt_diff
                        else:
                            altitude_gain_per_user[user] = alt_diff

                prev_alt = alt
                prev_activity = activity

            top_20 = sorted(altitude_gain_per_user.items(), key = lambda x: x[1], reverse = True)[:20]

            print("\nUser ID | Total Meters Gained")
            for user, gain in top_20:
                print(f"{user}     | {gain:,.0f} m")

            '''
            from the output we can see that user 128 has the most altitude gain.
            makes sense because the user has all airplaines records (3/3) 
            in addition to being a user with a lot of activities.
            '''

    def query_9(self):  
        '''
            Find all users who have invalid activities, and the number of invalid activities per user
            - An invalid activity is defined as an activity with consecutive trackpoints where the
            timestamps deviate with at least 5 minutes. 
        '''

        print("Users with invalid activities and the number of invalid activities per user:")
        invalid_activities_query = """
            SELECT A.user_id, A.id AS activity_id, TP.id AS trackpoint_id, TP.date_time
            FROM TrackPoint TP
            JOIN Activity A ON TP.activity_id = A.id
            ORDER BY A.user_id, A.id, TP.date_time;
        """

        result = self.db.execute_query_limited(invalid_activities_query, None, 10, printOut=False)

        if result:
            invalid_activities = {}
            marked_invalid_activities = set()

            prev_user = None
            prev_activity = None
            prev_time = None

            for user, activity, _, time in result:
                if user != prev_user or activity != prev_activity:
                    prev_user = user
                    prev_activity = activity
                    prev_time = None

                if prev_time is not None:
                    time_diff = (time - prev_time).total_seconds() / 60.0
                    
                    if time_diff >= 5.0 and activity not in marked_invalid_activities:    
                        marked_invalid_activities.add(activity)
                    
                        if user in invalid_activities:
                            invalid_activities[user] += 1
                        else:
                            invalid_activities[user] = 1

                prev_time = time

            print("\nUser ID | Number of Invalid Activities")
            for user, count in invalid_activities.items():
                print(f"{user}     | {count}")
            print("Affected users =", len(invalid_activities.items()))


    def query_10(self):
        '''
        Find the users who have tracked an activity in the Forbidden City of Beijing.
        - In this question you can consider the Forbidden City to have coordinates that 
            correspond to: lat 39.916, lon 116.397
        '''

        # Extending 750 meters east-west and 960 meters north-south, The Forbidden City covers 720,000 square meters
        # Thus, sensible to take a tolerance of 400 meters in each direction

        print("Users who have tracked an activity in the Forbidden City of Beijing:")

        
        lat_tolerance = 0.00431 # roughly 480 (960/2) meters, north-south
        lon_tolerance = 0.00337 # roughly 375 (750/2) meters, east-west

        forbidden_city_query = f"""
            SELECT DISTINCT A.user_id
            FROM TrackPoint TP
            JOIN Activity A ON TP.activity_id = A.id
            WHERE TP.lat BETWEEN {39.916 - lat_tolerance} AND {39.916 + lat_tolerance}
            AND TP.lon BETWEEN {116.397 - lon_tolerance} AND {116.397 + lon_tolerance};
        """

        result = self.db.execute_query(forbidden_city_query)


    def query_11(self):
        '''
        Find all users who have registered transportation_mode and their most used transportation_mode. 
        - The answer should be on format (user_id, most_used_transportation_mode) sorted on user_id.
        - Some users may have the same number of activities tagged with e.g. walk and car. In this case
          it is up to you to decide which transportation mode to include in your answer (choose one).
        - Do not count the rows where the mode is null
        '''

        print("Users, transportation mode count and how much that transport has been used. (sorted)")

        transportation_mode_query = """
            SELECT user_id, transportation_mode, COUNT(*) AS mode_count
            FROM Activity
            WHERE transportation_mode IS NOT NULL
            GROUP BY user_id, transportation_mode
            ORDER BY user_id, mode_count DESC;
        """

        result = self.db.execute_query(transportation_mode_query)

        if result:
            user_most_used_transportation = {}

            for user_id, mode, count in result:
                if user_id not in user_most_used_transportation:
                    user_most_used_transportation[user_id] = (mode, count)

            print("Users who have registered transportation_mode and their most used transportation_mode:")
            print("\nUser ID | Most Used Transportation Mode")
            for user_id in sorted(user_most_used_transportation):
                print(f"{user_id}     | {user_most_used_transportation[user_id][0]}")



def main():
    
    queries = DBQueries()
    
    print("\n---------------------------------------------------------\n")
    print("Query 1:")
    queries.query_1()
    print("\n---------------------------------------------------------\n")
    print("Query 2:")
    queries.query_2()
    print("\n---------------------------------------------------------\n")
    print("Query 3:")
    queries.query_3()
    print("\n---------------------------------------------------------\n")
    print("Query 4:")
    queries.query_4()
    print("\n---------------------------------------------------------\n")
    print("Query 5:")
    queries.query_5()
    print("\n---------------------------------------------------------\n")
    print("Query 6:")
    queries.query_6()
    print("\n---------------------------------------------------------\n")
    print("Query 7:")
    queries.query_7()
    print("\n---------------------------------------------------------\n")
    print("Query 8:")
    queries.query_8()
    print("\n---------------------------------------------------------\n")
    print("Query 9:")
    queries.query_9()
    print("\n---------------------------------------------------------\n")
    print("Query 10:")
    queries.query_10()
    print("\n---------------------------------------------------------\n")
    print("Query 11:")
    queries.query_11()


if __name__ == '__main__':
    main()