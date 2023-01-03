#!/usr/bin/env bash

# This script is used for complete setup of the project in a fresh Ubuntu 20.04 LTS version
# Since ubuntu 20.04 has the python 3.8.10 version, we don't need to install it. 
# But please ensure the python version is 3.8.10
# It is assumed that the project is already cloned and current working directory is
# - home/rbl_backend
#        - backend
# Below script is to be run at this location: home/rbl_backend/backend

while getopts i: flag
do
    case "${flag}" in
        i) ip_address=${OPTARG};;
    esac
done

# Updating the packages
echo -e "\n======Updating the packages======\n"
sudo apt update

# Install pip
echo -e "\n======Installing pip3======\n"
sudo apt install -y python3-pip

# Install redis
echo -e "\n======Installing Redis======\n"
sudo apt-get install -y redis-server

# Install Postgres
echo -e "\n======Installing Posgresql======\n"
sudo apt install -y postgresql postgresql-contrib

# Configure the postgres user
echo -e "\n======Make the posgres super user======\n"
sudo -u postgres createuser --superuser prash_psql
echo -e "\n======Set the password for super user======\n"
sudo -u postgres psql -c "ALTER ROLE prash_psql WITH PASSWORD 'prash123';"

sleep 2

# Nginx Configuration
echo -e "\n======Installing Nginx======\n"
sudo apt install -y nginx

# Setting up the Nginx\
echo -e "\n======Setting up the Nginx Configuration - Generating the Nginx config file======\n"
sudo touch /etc/nginx/sites-available/rbl_backend

echo -e "\n======Setting up the Nginx Configuration - Writing the Nginx config file======\n"
sudo tee -a /etc/nginx/sites-available/rbl_backend <<EOF
server {
listen 80;
server_name $ip_address;

location / {
  include proxy_params;
  proxy_pass http://$ip_address:8000;
    }
}                
EOF

sleep 2

echo -e "\n======Enabling the configuration file======\n"
sudo ln -s /etc/nginx/sites-available/rbl_backend /etc/nginx/sites-enabled/

# Install virtualenv
echo -e "\n======Installing virtualenv======\n"
sudo pip3 install virtualenv

# Make virtualev
echo -e "\n======Creating the virtual environment======\n"
virtualenv venv 

# Activating the virtuenv
echo -e "\n======Starting the virtual environment======\n"
source venv/bin/activate

sleep 2

# Installing the dependencies
echo -e "\n======Installing the python packages======\n"
pip3 install -r requirements.txt


# Setting up the database
echo -e "\n======Setting up the database======\n"
echo -e "\n======Exporting the database variables to bashrc======\n"
sudo echo "" >> ~/.bashrc
sudo echo "export FLASK_APP=app.py" >> ~/.bashrc
sudo echo "export DATABASE_URL='postgresql://eplfcjzsjrlefx:bfd39aa631ea4971aa380f49dada5a6463a0439d0d977058cf7243b60610eae0@ec2-54-208-104-27.compute-1.amazonaws.com:5432/dbh4a6k8ork3tk'" >> ~/.bashrc
source ~/.bashrc

sleep 2

echo -e "\n======Creating the database======\n"
flask db init
flask db migrate
flask db upgrade

# Starting the redis server and postgresql server
echo -e "\n======Starting the redis server======\n"
sudo service redis-server start

echo -e "\n======Starting the postgresql server======\n"
sudo service postgresql start

echo -e "\n======Starting the gunicorn server======\n"
gunicorn --bind 0.0.0.0:8000 app:app