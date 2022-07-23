### Project Setup
0. Clone the repo and make a virtual environment `python -m venv venv` ( Ubuntu )
1. Ask the owner ( prashantdwivedi194@gmail.com ) for environment variables
2. Make .env file locally in main project folder `rbl-backend` and put those variables there.
3. Install the dependencies `pip install -r requirements.txt`
4. To run the project locally run `gunicorn --reload --bind 127.0.0.1:8000 app:app`
5. *NOTE*: When you insert and delete the data from database it will be done from production database. So keep that thing in mind before deleting anything

### Database Setup
1. Install the postgresql in your system. To make sure your postgres is running properly follow below.
2. Step1: Running pg_lsclusters will list all the postgres clusters running on your device
```
Ver Cluster Port Status Owner    Data directory               Log file
9.6 main    5432 online postgres /var/lib/postgresql/9.6/main /var/log/postgresql/postgresql-9.6-main.log
```
If status is down, then follow step 3

3. Step 2: Restart the pg_ctlcluster
```
#format is pg_ctlcluster <version> <cluster> <action>
sudo pg_ctlcluster 9.6 main start

#restart postgresql service
sudo service postgresql restart 
```
4. Once postgres is running properly on your machine; make a super user named `prash_psql` and give the password `prash123 ` as this is what we are using to make the Database URL for local setup.

5. For Initial setup run the following commands
```
flask db init ( only must time when no migrations folder is present)
flask db migrate
flask db upgrade
```
Note: The final version your migration is what that will be used to update the database on heroku. So keep a note that the final version of migration is what you need in production.

### Database Updates
1. Make your changes in schema
2. Run the local migration command so that we can generate the migration versions that can be used later to update the db on heroku
`flask db migrate`
3. Then simply push your code to master, the moment code comes to master, it will be automatically deployed to heroku.
4. Once the build is finished and code is deployed, now you can upgrade the db on heroku using the migrations we have generated locally earlier in step 2
`heroku run flask db upgrade` Run this in your terminal

### Database Visualization
1. **On Production**
Run `heroku pg:psql --a rbl-backend` to see the database in terminal

2. **For local db**

Run `sudo -u postgres psql`. This will open the psql command line

### Basic DB commands
```
1. postgres=# \c rbl_backend;  --> to change the database
2. postgres=# \l --> To list the tables
3. Normal SQL Commands
```
