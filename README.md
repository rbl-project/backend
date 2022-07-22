### Project Setup
0. Clone the repo and make a virtual environment `python -m venv venv` ( Ubuntu )
1. Ask the owner ( prashantdwivedi194@gmail.com ) for environment variables
2. Make .env file locally in main project folder `rbl-backend` and put those variables there.
3. Install the dependencies `pip install -r requirements.txt`
4. To run the project locally run `gunicorn --reload --bind 127.0.0.1:8000 app:app`
5. *NOTE*: When you insert and delete the data from database it will be done from production database. So keep that thing in mind before deleting anything

### Database Updates
1. Make your changes in schema
2. Run the local migration command so that we can generate the migration versions that can be used later to update the db on heroku
`flask db migrate`
3. Then simply push your code to master, the moment code comes to master, it will be automatically deployed to heroku.
4. Once the build is finished and code is deployed, now you can upgrade the db on heroku using the migrations we have generated locally earlier in step 2
`heroku run flask db upgrade` Run this in your terminal

### Database Visualization
Run `heroku pg:psql --a rbl-backend` to see the database in terminal