import os
import random
import math

def explore_dataset(dataset_path):
    # Iterate through each user folder (000, 001, ..., 181)
    data_folder_path = os.path.join(dataset_path, 'Data')
    for user_folder in os.listdir(data_folder_path):
        user_folder_path = os.path.join(data_folder_path, user_folder)

        # Verify that the current path is a directory
        if os.path.isdir(user_folder_path):
            trajectory_folder_path = os.path.join(user_folder_path, 'Trajectory')
            
            # Check if Trajectory folder exists
            if os.path.exists(trajectory_folder_path):
                for plt_file in os.listdir(trajectory_folder_path):
                    if plt_file.endswith(".plt"):
                        plt_file_path = os.path.join(trajectory_folder_path, plt_file)
                        
                        # Open and read the .plt file
                        with open(plt_file_path, 'r') as file:
                            print(f"\nReading file: {plt_file_path}")
                            lines = file.readlines()
                            
                            # Skip the first 6 lines if they are headers (common in Geolife .plt files)
                            data_lines = lines[6:]
                            
                            # Check each line for missing values
                            for line in data_lines[:10]:  # Checking first 10 lines as an example
                                fields = line.strip().split(',')
                                missing_fields = [i for i, field in enumerate(fields) if field == '']
                                
                                if missing_fields:
                                    print(f"Line with missing values: {line.strip()}")
                                    print(f"Missing fields at positions: {missing_fields}")
                                else:
                                    print(f"Line is complete: {line.strip()}")

def count_plt_files(dataset_path):
    # Initialize the count of .plt files
    total_plt_count = 0
    user_plt_count = {}

    # Iterate through each user folder
    data_folder_path = os.path.join(dataset_path, 'Data')
    for user_folder in os.listdir(data_folder_path):
        user_folder_path = os.path.join(data_folder_path, user_folder)

        # Verify that the current path is a directory
        if os.path.isdir(user_folder_path):
            trajectory_folder_path = os.path.join(user_folder_path, 'Trajectory')
            
            # Check if Trajectory folder exists
            if os.path.exists(trajectory_folder_path):
                plt_count = sum(1 for plt_file in os.listdir(trajectory_folder_path) if plt_file.endswith(".plt"))
                
                # Store the count for each user
                user_plt_count[user_folder] = plt_count
                
                # Add to the total count
                total_plt_count += plt_count

    print("\nSummary of .plt files for each user:")
    for user, count in user_plt_count.items():
        print(f"User {user}: {count} .plt files")

    print(f"\nTotal number of .plt files: {total_plt_count}")

def count_lines_in_plt_files(dataset_path):
    # Dictionary to store the number of lines for each .plt file
    plt_line_counts = {}

    # Iterate through each user folder (000, 001, ..., 181)
    data_folder_path = os.path.join(dataset_path, 'Data')
    for user_folder in os.listdir(data_folder_path):
        user_folder_path = os.path.join(data_folder_path, user_folder)

        # Verify that the current path is a directory
        if os.path.isdir(user_folder_path):
            trajectory_folder_path = os.path.join(user_folder_path, 'Trajectory')
            
            # Check if Trajectory folder exists
            if os.path.exists(trajectory_folder_path):
                user_total_lines = 0
                for plt_file in os.listdir(trajectory_folder_path):
                    if plt_file.endswith(".plt"):
                        plt_file_path = os.path.join(trajectory_folder_path, plt_file)
                        
                        # Count the number of lines in the .plt file, excluding header lines
                        with open(plt_file_path, 'r') as file:
                            lines = file.readlines()
                            data_lines = lines[6:]  # Skip the first 6 header lines (Geolife format)
                            line_count = len(data_lines)
                            plt_line_counts[plt_file_path] = line_count
                            user_total_lines += line_count

                # Print the total line count for this user folder
                print(f"Total number of lines in all .plt files for user {user_folder}: {user_total_lines}")

    # Print line count for each file
    print("\nSummary of line counts in each .plt file:")
    for plt_file, count in plt_line_counts.items():
        print(f"{plt_file}: {count} data lines")

def print_random_plt_file(dataset_path):
    # List to store all plt file paths
    plt_files = []

    # Traverse through the dataset to collect all .plt files
    data_folder_path = os.path.join(dataset_path, 'Data')
    for user_folder in os.listdir(data_folder_path):
        user_folder_path = os.path.join(data_folder_path, user_folder)

        # Verify that the current path is a directory
        if os.path.isdir(user_folder_path):
            trajectory_folder_path = os.path.join(user_folder_path, 'Trajectory')
            
            # Check if Trajectory folder exists
            if os.path.exists(trajectory_folder_path):
                for plt_file in os.listdir(trajectory_folder_path):
                    if plt_file.endswith(".plt"):
                        plt_file_path = os.path.join(trajectory_folder_path, plt_file)
                        plt_files.append(plt_file_path)

    # Select a random plt file
    if plt_files:
        random_plt_file = random.choice(plt_files)
        print(f"\nRandomly selected file: {random_plt_file}")

        # Open and read the entire contents of the .plt file
        with open(random_plt_file, 'r') as file:
            lines = file.readlines()
            for line in lines:
                print(line.strip())
    else:
        print("No .plt files found in the dataset.")


dataset_path = "dataset/dataset"


#explore_dataset(dataset_path)

#print_random_plt_file (dataset_path)



def haversine(lat1, lon1, lat2, lon2):
    
    # Radius of Earth in km
    R = 6371.0
    R_m = R * 1000
    
    # Convert latitude and longitude from degrees to radians
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    # Haversine formula
    a = math.sin(delta_phi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2.0) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return (R_m * c)  # Distance in meters