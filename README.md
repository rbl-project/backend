# Backend for RBL Project - Web Based UI Portal for Complete Data Engineering
### Ubuntu Setup in Windows using WSL.
1. Switch on the WSL form `Windows Switch on/off feature`, you can search this in search bar. Restart PC.
To make sure WSL is running, run `wsl -l -v` in Poswershell. It must say that no distros are installed.
2. Install Ubuntu Distro from Microsoft Store: https://apps.microsoft.com/store/detail/ubuntu-2004/9N6SVWS3RX71?hl=en-in&gl=IN
3. Run Unbuntu and set up the username and password.
4. Install `Python 3.8`

### Project Setup
0. Make a `rbl_backend` folder in home directory. Inside that directory make the virutal environment
`python3 -m venv venv`. You might be prompted to intall venv package, follow the given command there, and install the package and run the command again. Now, Clone the repo in same directory.
1. Ask the owner ( prashantdwivedi194@gmail.com ) for environment variables
2. Make .env file locally in main project folder `rbl-backend` and put those variables there.
3. Install the dependencies `pip install -r requirements.txt`
4. To run the project locally run `gunicorn --reload --bind 127.0.0.1:8000 app:app`. ( Bedfore this the DB setup must be done. Follow below)

### Database Setup
1. Install the postgresql in your system. 
```
# updates
sudo apt update

# install postgres 
sudo apt install postgresql postgresql-contrib
```
To make sure your postgres is running properly follow below.

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
4. Once postgres is running properly on your machine; make a super user named `prash_psql` and give the password `prash123 ` as this is what we are using to make the Database URL for local setup. Copy below commands as it is.
```
1. Make the super user
sudo -u postgres createuser --superuser prash_psql

2. Set the password
sudo -u postgres psql -c "ALTER ROLE prash_psql WITH PASSWORD 'prash123';"
```

5. For Initial Database Setup run the following commands. ( **NOTE** Before doing this make sure you have set FLASK_APP and DATABSE_URL environment variables in `.basrc` file.)
Eg:( You can copy the same )
```
export FLASK_APP=app.py

export DATABASE_URL=postgresql://eplfcjzsjrlefx:bfd39aa631ea4971aa380f49dada5a6463a0439d0d977058cf7243b60610eae0@ec2-54-208-104-27.compute-1.amazonaws.com:5432/dbh4a6k8ork3tk

```

Once above variables are set, then run the below command
```
flask db init ( only must time when no migrations folder is present)
flask db migrate
flask db upgrade
```
Note: The final version your migration is what that will be used to update the database on heroku. So keep a note that the final version of migration is what you need in production.

### Database Schema Updates
1. Make your changes in schema
2. Run the local migration command so that we can generate the migration versions that can be used later to update the db on heroku
```
1. flask db migrate
2. flask db upgrade

```
3. Then simply push your code to master, the moment code comes to master, it will be automatically deployed to heroku.
4. Once the build is finished and code is deployed, now you can upgrade the db on heroku using the migrations we have generated locally earlier in step 2
`heroku run flask db upgrade` Run this in your terminal.

*NOTE*  DO NOT PUSH MULTIPLE MIGRATIONS ON HEROKU! THIS WOULD SIMPLY DESTROY THE HEROKU DATABASE AND CREATE A LOT OF PROBLEMS DUE TO MISS MATCH OF VERSION ON HEROKU AND LOCAL.

### Database Visualization
1. **On Production**
Run `heroku pg:psql --app rbl-backend` to see the database in terminal

2. **For local db**

Run `sudo -u postgres psql`. This will open the psql command line

### Basic DB commands
```
1. postgres=# \c rbl_backend;  --> to change the database
2. postgres=# \l --> To list the databases
3. postgres=# \d --> To list all the tables in current database
3. Normal SQL Commands
```
### Database Troubleshooting

If at all any error happens while upgrading the database on heroku using `heroku run flask db upgrade` then we having conflicting version in alembic_table and migrations folders.

In this case kidly drop the table alembic_version and run `heroku run flask db upgrade` again to make the table with latest schema.
