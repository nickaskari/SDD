import os

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
                            
                            # Print the first few lines of data to understand the structure
                            for line in data_lines[:5]:
                                print(line.strip())

                            # You can add more logic here to analyze the data
                            # e.g., check for NULL values, data consistency, etc.

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


dataset_path = "/Users/nickaskari/Desktop/dataset/dataset"


#explore_dataset(dataset_path)

count_plt_files(dataset_path)
