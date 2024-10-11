# SDD

# Setting Up Docker for the Project

This guide will help you set up the Docker environment for our project. Follow the steps below to get the MySQL database up and running.


### Step 1: Run Docker Compose
Once you have Docker installed and your `.env` file is ready, run the following command in the root of the repository to start the MySQL container:

```sh
docker-compose up -d
```

This command will:
- Build and start the MySQL container in detached mode (`-d` flag).

### Step 2: Verify the Setup
To verify that the container is running, use:

```sh
docker ps
```

You should see `mysql-container` listed as one of the running containers.

### Step 3 (Optional): Access MySQL Container
If you need to access the MySQL instance inside the container, use the following command:

```sh
docker exec -it mysql-container mysql -u root -p
```

You will be prompted to enter the MySQL root password (use the password from the `.env` file).

### Step 4: Stop the Docker Containers
When you're done and want to stop the running containers, use:

```sh
docker-compose down
```

# Setting Up Virtual Environment

Creating a virtual environment called sddpr1
```sh
python3.10 -m venv sddpr1 
```

Activates the virtual environment sddpr1 for use.
```sh
source sddpr1/bin/activate
```

# Do This When New Packages (either by you or others)

If you plan to use a new package..
```sh
pip install <package-name>
```

Then you add that package to the requirments.txt file as follows,

```sh
pip freeze > requirements.txt
```

If you pull from repository, and someone has used a new package, do this when inside your virtual environment,
```sh
pip install -r requirements.txt
```
