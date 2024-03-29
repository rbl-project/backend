# Backend for RBL Project - Web Based UI Portal for Complete Data Engineering
### Ubuntu Setup in Windows using WSL.
1. Switch on the WSL form `Windows Switch on/off feature`, you can search this in search bar. Restart PC.
To make sure WSL is running, run `wsl -l -v` in Poswershell. It must say that no distros are installed.
2. Install Ubuntu Distro from Microsoft Store: https://apps.microsoft.com/store/detail/ubuntu-2004/9N6SVWS3RX71?hl=en-in&gl=IN
3. Run Unbuntu and set up the username and password.
4. Install `Python 3.8.10` [ STRICKLY 3.8.10 ]

### Project Setup
1. Make a `rbl_backend` folder in home directory. Inside this directory make the virutal environment using command `python3 -m venv venv`. [ You might be prompted to intall venv package, follow the given command `sudo apt-get install python3-virtualenv` , and install the package and run the command again. ]
2. Now, Clone the repo in same directory. After cloning the directry structure should look like this.
```
rbl_backend
    - backend
    - venv
```
Please ensure that you have same directory structure as above for the project to work properly.

3. Download the `.env` file from resources repository or ask the owner ( prashantdwivedi194@gmail.com ) for environment variables file.

4. Make sure that you have `.env` file availale in `backend` directory.

5. Install the dependencies `pip install -r requirements.txt`

6. Install Redis In Your Linux System using below commands.
```
sudo apt update

sudo apt-get install -y redis-server
```

### First Time Local Database Setup

1. Install the postgresql in your linux system. 
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

### First Time Prod DB Setup [ THIS IS STALE NOW. THIS WAS IN CASE OF HEROKU BUT NOW WE ARE USING RENDER.COM]
1. Run `heroku run python -a rbl-backend` in another terminal.
2. Now in python shell run the below command.
```
from app import create_app
app = create_app()
app.app_context().push()
db.create_all()

```

### Database Visualization

1. **On Production**

Database is present on `render.com`. Use the below command in terminal to open the database.( Make sure postgres is installed in your system and running. If not running then restart postgresql service using `sudo service postgresql restart`)

```PGPASSWORD=fqOlZ3qSFZjHzfe5OmlgEMKa0siBKqON psql -h dpg-cek0asda4991ihntel5g-a.oregon-postgres.render.com -U rbl_user rbl_db```

2. **For local db**

Run `sudo -u postgres psql`. This will open the psql command line


### Database Schema Updates
- Local Database:
1. Make your changes in schema
2. Run the local migration command so that we can generate the migration versions that can be used later to update the db on heroku
```
1. flask db migrate
2. flask db upgrade

```

- Prod Database:
1. Make your changes in schema manually by opening the database in terminal using the above command( in section **Database Visualization**)


### Basic DB commands
```
1. postgres=# \c rbl_backend;  --> to change the database
2. postgres=# \l --> To list the databases
3. postgres=# \d --> To list all the tables in current database
4. postgres=# \d+ table_name --> To get the schema of a table
5. postgres=# create table users( id serial primary key not null, name varchar(200) not null, email varchar(120) not null, db_count int, date_added timestamp, password_hash varchar(128) );
```
### Database Troubleshooting

If at all any error happens while upgrading the database on heroku using `heroku run flask db upgrade` then we having conflicting version in alembic_table and migrations folders.

In this case kidly drop the table alembic_version and run `heroku run flask db upgrade` again to make the table with latest schema.


### Final Run After Project Setup
1. Start redis server by running `sudo service redis-server start`
2. Start postgres database by running `sudo service postgresql restart`
3. Start celery worker by running `celery -A app.celery_instance worker --loglevel=info`
4. Start the gunicorn using `gunicorn --reload --bind 127.0.0.1 app:app --timeout 999999999`
