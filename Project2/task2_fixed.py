from task1 import DBManager
from Haversine import haversine
from datetime import datetime
from tabulate import tabulate
from collections import defaultdict


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
            
            { "$addFields": {
                    "date_time": {
                        "$dateFromString": {
                            "dateString": "$date_time",
                            "format": "%Y-%m-%d %H:%M:%S"
                        }
                    }
                }
            },

            { "$addFields": {
                    "year": { "$year": "$date_time" }
                }
            },

            { "$group": {
                    "_id": { "year": "$year", "activity_id": "$activity_id" },
                    "count": { "$sum": 1 }  # Ensures unique activity_id per year is counted once
                }
            },
            
            { "$group": {
                    "_id": "$_id.year", 
                    "count": { "$sum": 1 } 
                    } 
            },

            { "$sort": { "count": -1 } },
            
            { "$limit": 1 }
        ]
        
        result = list(self.db.db.TrackPoint.aggregate(pipeline))

        if result:
            print(f"The year with the most activities: {result[0]['_id']} with {result[0]['count']} activities")

    def query_6b(self):
        pipeline = [
            { "$addFields": {
                    "date_time": {
                        "$dateFromString": {
                            "dateString": "$date_time",
                            "format": "%Y-%m-%d %H:%M:%S"
                        }
                    }
                }
            },

            { "$sort": {
                    "activity_id": 1,
                    "date_time": 1
                }
            },

            { "$group": {
                    "_id": {
                        "activity_id": "$activity_id",
                        "year": { "$year": "$date_time" }
                    },
                    "start_time": { "$first": "$date_time" },
                    "end_time": { "$last": "$date_time" }
                }
            },

            { "$addFields": {
                    "hours": {
                        "$divide": [
                            { "$subtract": ["$end_time", "$start_time"] }, 3600000.0  # Convert milliseconds to hours
                        ]
                    }
                }
            },

            { "$group": {
                    "_id": "$_id.year",  # Group by year
                    "total_hours": { "$sum": "$hours" }  # Sum the total hours for each year
                }
            },

            { "$sort": { "total_hours": -1 } },

            { "$limit": 1 }
        ]

        result = list(self.db.db.TrackPoint.aggregate(pipeline))

        if result:
            print(f"The year with the most recorded hours: {result[0]['_id']} with {result[0]['total_hours']:.2f} hours")



    def query_7(self):
        activities_2008 = list(self.db.db.Activity.find({
            "user_id": "112",
            "transportation_mode": "walk",
            "$expr": {
                "$and": [
                    { "$eq": [{ "$year": { "$dateFromString": { "dateString": "$start_date_time" }}}, 2008] },
                    { "$eq": [{ "$year": { "$dateFromString": { "dateString": "$end_date_time" }}}, 2008] }
                ]
            }
        }, ))

        total_distance = 0

        # Creating an index to make it fast
        self.db.db.TrackPoint.create_index([("activity_id", 1), ("date_time", 1)])

        for activity in activities_2008:
            activity_id = activity["_id"]

            trackpoints = list(self.db.db.TrackPoint.find(
                { "activity_id": activity_id },
            ).sort("date_time", 1))

            for i in range(1, len(trackpoints)):
                prev_point = trackpoints[i - 1]
                curr_point = trackpoints[i]

                lat1, lon1 = prev_point["lat"], prev_point["lon"]
                lat2, lon2 = curr_point["lat"], curr_point["lon"]

                if lat1 is not None and lon1 is not None and lat2 is not None and lon2 is not None:
                    distance = haversine(lat1, lon1, lat2, lon2)
                    total_distance += distance

        print(f"Total distance walked in 2008 by user 112: {total_distance:.2f} km")



    def query_8(self):
        pipeline = [
        # Step 1: Join TrackPoint with Activity on activity_id to get user_id
        {"$lookup": {
        "from": "Activity",
        "localField": "activity_id",
        "foreignField": "_id",
        "as": "activity"
        }},
        {"$unwind": "$activity"},  # Unwind the joined Activity array
        {"$addFields": {"user_id": "$activity.user_id", "altitude_meters": {"$multiply": ["$altitude", 0.3048]}}},  # Convert altitude to meters

        # Step 2: Sort by user_id, activity_id, and date_time
        {"$sort": {"user_id": 1, "activity_id": 1, "date_time": 1}},

        # Step 3: Group trackpoints by user and activity, keeping track of consecutive altitudes
        {"$group": {
        "_id": {"user_id": "$user_id", "activity_id": "$activity_id"},
        "altitudes": {"$push": {"altitude": "$altitude_meters", "date_time": "$date_time"}}  # Store altitudes and timestamps
        }},

        # Step 4: Process each group to calculate altitude gain for each activity
        {"$project": {
        "altitude_gain": {
            "$reduce": {
                "input": "$altitudes",
                "initialValue": {"sum": 0, "prev_alt": None},
                "in": {
                    "$cond": [
                        {"$and": [{"$ne": ["$$value.prev_alt", None]}, {"$gt": ["$$this.altitude", "$$value.prev_alt"]}]},
                        {"sum": {"$add": ["$$value.sum", {"$subtract": ["$$this.altitude", "$$value.prev_alt"]}]}, "prev_alt": "$$this.altitude"},
                        {"sum": "$$value.sum", "prev_alt": "$$this.altitude"}
                    ]
                }
            }
        }
        }},

        # Step 5: Group by user_id to sum altitude gain across all activities
        {"$group": {
        "_id": "$_id.user_id",
        "total_altitude_gain": {"$sum": "$altitude_gain.sum"}
        }},

        # Step 6: Sort by total altitude gain in descending order and limit to top 10 users
        {"$sort": {"total_altitude_gain": -1}},
        {"$limit": 10}
        ]
        
        result = list(self.db.db.TrackPoint.aggregate(pipeline))
        if result:
            print("Top 10 users with the most altitude gain (in meters):")
            for user in result:
                print(f"User {user['_id']}: {user['total_altitude_gain']:.2f} meters")
        else:
            print("No data found.")


    def query_9(self):
        invalid_activity_count = defaultdict(int)

        users = self.db.db.User.find({})

        for user in users:
            user_id = user["_id"]
            
            activities = self.db.db.Activity.find({"_id": {"$in": user["activities"]}})
            
            for activity in activities:
                trackpoints = activity.get("trackpoints", [])
                
                for i in range(1, len(trackpoints)):
                    prev_time_str = trackpoints[i - 1]["date_time"]
                    curr_time_str = trackpoints[i]["date_time"]

                    prev_time = datetime.strptime(prev_time_str, "%Y-%m-%d %H:%M:%S")
                    curr_time = datetime.strptime(curr_time_str, "%Y-%m-%d %H:%M:%S")

                    time_diff_minutes = (curr_time - prev_time).total_seconds() / 60

                    if time_diff_minutes >= 5:  
                        invalid_activity_count[user_id] += 1
                        break  

        output_data = [(user_id, count) for user_id, count in invalid_activity_count.items()]
        output_data_sorted = sorted(output_data, key=lambda x: x[0], reverse=False)  

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

        # Find all activities with trackpoints that are within the bounds
        activities_with_forbidden_trackpoints = self.db.db.Activity.find({
            "trackpoints": {
                "$elemMatch": forbidden_city_bounds  
            }
        })

        users_in_forbidden_city = set()

        for activity in activities_with_forbidden_trackpoints:
            user_id = activity.get("user_id")
            trackpoints = activity.get("trackpoints", [])

            for trackpoint in trackpoints:
                lat, lon = trackpoint.get("lat"), trackpoint.get("lon")

                if 39.912 <= lat <= 39.920 and 116.393 <= lon <= 116.401:
                    users_in_forbidden_city.add(user_id)
                    break  # No need to check more trackpoints for this activity

        print("Users with activities in the Forbidden City:")
        for user_id in users_in_forbidden_city:
            print(f"User ID: {user_id}")


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

    queries.query_8()

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