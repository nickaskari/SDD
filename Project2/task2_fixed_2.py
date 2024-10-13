from task1 import DBManager
from Haversine import haversine
from datetime import datetime
from tabulate import tabulate
from collections import defaultdict
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed


class DBQueries:
    
    def __init__(self):
        self.db = DBManager()

    def query_1(self):
        collections = ["User", "Activity", "TrackPoint"]
        for collection in collections:
            count = self.db.db[collection].count_documents({})
            print(f"{collection} has {count} documents.")

    def query_2(self):
        pipeline = [
            {"$unwind": "$activities"},
            {"$group": {"_id": "$_id", "activity_count": {"$sum": 1}}},
            {"$group": {"_id": None, "avg_activity_count": {"$avg": "$activity_count"}}}
        ]
        result = list(self.db.db.User.aggregate(pipeline))
        print(f"Average number of activities per user: {result[0]['avg_activity_count']:.4f}")

    def query_3(self):
        pipeline = [
            {"$unwind": "$activities"},
            {"$group": {"_id": "$_id", "activity_count": {"$sum": 1}}},
            {"$sort": {"activity_count": -1}},
            {"$limit": 20}
        ]
        result = list(self.db.db.User.aggregate(pipeline))
        print("Top 20 users with the highest number of activities:")
        for user in result:
            print(f"User {user['_id']}: {user['activity_count']} activities")

    def query_4(self):
        taxi_users = self.db.db.Activity.distinct("user_id", {"transportation_mode": "taxi"})
        print("Users who have taken a taxi:")
        for user in taxi_users:
            print(f"{user}")

    def query_5(self):
        pipeline = [
            {"$match": {"transportation_mode": {"$ne": None}}},
            {"$group": {"_id": "$transportation_mode", "count": {"$sum": 1}}}
        ]
        result = list(self.db.db.Activity.aggregate(pipeline))
        result.sort(key=lambda x: x["count"], reverse=True)
        print("Transportation modes and their counts:")
        for mode in result:
            print(f"{mode['_id']}: {mode['count']}")

    def query_6a(self):
        pipeline = [
            # Convert start_date_time from string to date
            {"$addFields": {
                "start_date_time": {
                    "$dateFromString": {
                        "dateString": "$start_date_time",
                        "format": "%Y-%m-%d %H:%M:%S"
                    }
                }
            }},
            {"$project": {"year": {"$year": "$start_date_time"}}},
            
            {"$group": {"_id": "$year", "count": {"$sum": 1}}},
            
            {"$sort": {"count": -1}},
            {"$limit": 1}
        ]
        result = list(self.db.db.Activity.aggregate(pipeline))

        if result:
            print(f"The year with the most activities: {result[0]['_id']} with {result[0]['count']} activities")
        else:
            print("fail")

    def query_6b(self):
        pipeline = [
            # Convert start_date_time and end_date_time from strings to dates
            {"$addFields": {
                "start_date_time": {
                    "$dateFromString": {
                        "dateString": "$start_date_time",
                        "format": "%Y-%m-%d %H:%M:%S"
                    }
                },
                "end_date_time": {
                    "$dateFromString": {
                        "dateString": "$end_date_time",
                        "format": "%Y-%m-%d %H:%M:%S"
                    }
                }
            }},
            # Project the year and duration in milliseconds because of the format
            {"$project": {
                "year": {"$year": "$start_date_time"}, 
                "duration": {"$subtract": ["$end_date_time", "$start_date_time"]}
            }},
            # Group by year and calculate total hours
            {"$group": {"_id": "$year", "total_hours": {"$sum": {"$divide": ["$duration", 3600000]}}}},
            # Sort by total hours in descending order
            {"$sort": {"total_hours": -1}},
            {"$limit": 1}
        ]
        
        result = list(self.db.db.Activity.aggregate(pipeline))

        # Display the result
        if result:
            print(f"The year with the most recorded hours: {result[0]['_id']} with {result[0]['total_hours']:.2f} hours")
        else:
            print("fail")


    def query_7(self):
        pipeline = [
            {"$match": {
                "user_id": "112", "transportation_mode": "walk",
                "start_date_time": {"$gte": "2008-01-01 00:00:00", "$lt": "2009-01-01 00:00:00"}
            }},
            {"$unwind": "$trackpoints"},
            {"$lookup": {
                "from": "TrackPoint", "localField": "trackpoints", "foreignField": "_id", "as": "tp"
            }},
            {"$unwind": "$tp"},
            {"$project": {"lat": "$tp.lat", "lon": "$tp.lon", "date_time": "$tp.date_time"}},
            {"$sort": {"date_time": 1}},
            {"$group": {"_id": "$_id", "trackpoints": {"$push": {"lat": "$lat", "lon": "$lon"}}}}
        ]

        result = list(self.db.db.Activity.aggregate(pipeline))
        total_distance_km = 0

        if result:
            for activity in result:
                tps = activity['trackpoints']
                for i in range(1, len(tps)):
                    total_distance_km += haversine(tps[i-1]['lat'], tps[i-1]['lon'], tps[i]['lat'], tps[i]['lon'])

            print(f"Total distance walked in 2008 by user 112: {total_distance_km:.2f} km")
        else:
            print("fail")


    def process_activity_q8(self, activity):
        user_id = activity["user_id"]
        trackpoint_ids = activity["trackpoints"]

        # Fetch corresponding TrackPoints by ID
        self.db.db.TrackPoint.create_index([("date_time", 1)])

        trackpoints = list(self.db.db.TrackPoint.find(
            {"_id": {"$in": trackpoint_ids}}
        ).sort("date_time", 1))

        prev_altitude = None
        altitude_gain = 0

        # Calculate altitude gain for the activity
        for tp in trackpoints:
            altitude = tp.get("altitude")

            if altitude is not None and prev_altitude is not None:
                alt_diff = altitude - prev_altitude
                if alt_diff > 0.0: 
                    altitude_gain += alt_diff * 0.3048  # Convert to meters

            prev_altitude = altitude

        return user_id, altitude_gain

    def query_8(self):
        total_activities = self.db.db.Activity.count_documents({
            "trackpoints": {"$exists": True, "$ne": []}
        })

        activities = self.db.db.Activity.find({
            "trackpoints": {"$exists": True, "$ne": []}
        })

        altitude_gain_per_user = {}

        # Use ThreadPoolExecutor to process activities in parallel
        with ThreadPoolExecutor(max_workers=4) as executor:
            # All alltitude gains for every activity (a user could have a lot of activities)
            futures = [executor.submit(self.process_activity_q8, activity) for activity in activities]

            for future in tqdm(as_completed(futures), total=total_activities, desc="Processing Query 8"):
                user_id, altitude_gain = future.result()

                if altitude_gain > 0.0:
                    altitude_gain_per_user[user_id] = altitude_gain_per_user.get(user_id, 0) + altitude_gain

        top_10_users = sorted(altitude_gain_per_user.items(), key=lambda x: x[1], reverse=True)[:10]
        print("\nTop 10 users with the most altitude gain:")
        for user, gain in top_10_users:
            print(f"User {user}: {gain:,.0f} meters")


    def process_activity_q9(self, user_id, activity):
        trackpoint_ids = activity.get("trackpoints", [])

        if len(trackpoint_ids) < 2:
            return None

        # Fetch only `date_time` field of trackpoints in bulk, sorted by `date_time`
        trackpoints = list(self.db.db.TrackPoint.find(
            {"_id": {"$in": trackpoint_ids}},
            {"date_time": 1}  
        ).sort("date_time", 1))

        for i in range(1, len(trackpoints)):
            prev_time_str = trackpoints[i - 1]["date_time"]
            curr_time_str = trackpoints[i]["date_time"]

            prev_time = datetime.strptime(prev_time_str, "%Y-%m-%d %H:%M:%S")
            curr_time = datetime.strptime(curr_time_str, "%Y-%m-%d %H:%M:%S")

            time_diff_minutes = (curr_time - prev_time).total_seconds() / 60

            if time_diff_minutes >= 5:
                return user_id  # Return user_id if invalid activity is found

        return None  # No invalid activity found

    def query_9(self):
        invalid_activity_count = defaultdict(int)

        users = list(self.db.db.User.find({}, {"_id": 1, "activities": 1}))

        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = []
            for user in users:
                user_id = user["_id"]
                activities = list(self.db.db.Activity.find({"_id": {"$in": user["activities"]}}, {"trackpoints": 1}))

                for activity in activities:
                    futures.append(executor.submit(self.process_activity_q9, user_id, activity))

            for future in tqdm(as_completed(futures), total=len(futures), desc="Processing Query 9"):
                user_id = future.result()
                if user_id is not None:
                    invalid_activity_count[user_id] += 1

        output_data = [(user_id, count) for user_id, count in invalid_activity_count.items()]
        output_data_sorted = sorted(output_data, key=lambda x: x[0], reverse=False)  # Sort by user_id

        print(tabulate(output_data_sorted, headers=["User ID", "Number of Invalid Activities"], tablefmt="plain"))


    def query_10(self):
        '''
        lat_tolerance = 0.00431 # roughly 480 (960/2) meters, north-south
        lon_tolerance = 0.00337 # roughly 375 (750/2) meters, east-west
        '''

        forbidden_city_bounds = {
            "lat": {"$gte": 39.912, "$lte": 39.920},
            "lon": {"$gte": 116.393, "$lte": 116.401}
        }

        pipeline = [
            {"$lookup": {
                "from": "TrackPoint", "localField": "trackpoints", "foreignField": "_id", "as": "trackpoint_data"
            }},
            {"$match": {
                "trackpoint_data": {
                    "$elemMatch": {"lat": {"$gte": 39.912, "$lte": 39.920}, "lon": {"$gte": 116.393, "$lte": 116.401}}
                }
            }},
            {"$group": {"_id": "$user_id"}}
        ]
        
        users_in_forbidden_city = list(self.db.db.Activity.aggregate(pipeline))

        print("Users with activities in the Forbidden City:")
        for user in users_in_forbidden_city:
            print(f"User ID: {user['_id']}")


    def query_11(self):
        pipeline = [
            {"$match": {"transportation_mode": {"$ne": None}}},
            {"$group": {
                "_id": {"user_id": "$user_id", "transportation_mode": "$transportation_mode"}, 
                "count": {"$sum": 1}
            }},
            
            {"$sort": {"_id.user_id": 1, "count": -1}},
            
            {"$group": {
                "_id": "$_id.user_id", 
                "most_used_mode": {"$first": "$_id.transportation_mode"},
                "count": {"$first": "$count"}
            }},
            
            {"$sort": {"_id": 1}}
        ]

        result = list(self.db.db.Activity.aggregate(pipeline))
        
        print("Users and their most used transportation mode:")
        for user in result:
            print(f"User {user['_id']}: {user['most_used_mode']} ({user['count']} times)")


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
    print("Query 6a:")
    queries.query_6a()
    print("\n---------------------------------------------------------\n")
    print("Query 6b:")
    queries.query_6b()
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