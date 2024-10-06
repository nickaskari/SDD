from DbConnector import DbConnector
from tabulate import tabulate
from tqdm import tqdm
from task1 import DBManager

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

    # PART B) IS NOT FINISHED
    def query_6(self):
        ''' 
            a) Finds the year with the most activities.
            b) Answers whether the year found is also the year with the most recorded hours.
        '''

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

        result = self.db.execute_query(most_activities_year_query)

        
        if result:
            activity_year, activity_count = result[0]
            print(f"The year with the most activities is {activity_year} with {activity_count} activities.")



def main():
    
    queries = DBQueries()

    queries.query_1()
    print("---------------------------------------------------------")
    queries.query_2()
    print("---------------------------------------------------------")
    queries.query_3()
    print("---------------------------------------------------------")
    queries.query_4()
    print("---------------------------------------------------------")
    queries.query_5()
    print("---------------------------------------------------------")
    # PART b) IS NOT FINISHED
    queries.query_6()

if __name__ == '__main__':
    main()