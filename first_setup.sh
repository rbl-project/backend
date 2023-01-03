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
echo -e "\nUpdating the packages"
sudo apt update

# Install pip
echo -e "\nInstalling pip3"
sudo apt install -y python3-pip

# Install redis
echo -e "\nInstalling Redis"
sudo apt-get install -y redis-server

# Install Postgres
echo -e "\nInstalling Posgresql"
sudo apt install -y postgresql postgresql-contrib

# Configure the postgres user
echo -e "\nMake the posgres super user"
sudo -u postgres createuser --superuser prash_psql
echo -e "\nSet the password for super user"
sudo -u postgres psql -c "ALTER ROLE prash_psql WITH PASSWORD 'prash123';"


# Nginx Configuration
echo -e "\nInstalling Nginx"
sudo apt install -y nginx

# Setting up the Nginx\
echo -e "\nSetting up the Nginx Configuration - Generating the Nginx config file"
sudo touch /etc/nginx/sites-available/rbl_backend

echo -e "\nSetting up the Nginx Configuration - Writing the Nginx config file\n"
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

echo -e "\nEnabling the configuration file"
sudo ln -s /etc/nginx/sites-available/rbl_backend /etc/nginx/sites-enabled/

# Install virtualenv
echo -e "\nInstalling virtualenv"
sudo pip3 install virtualenv

# Make virtualev
echo -e "\nCreating the virtual environment"
virtualenv venv 

# Activating the virtuenv
echo -e "\nStarting the virtual environment"
source venv/bin/activate

# Installing the dependencies
echo -e "\nInstalling the python packages"
pip3 install -r requirements.txt


# Setting up the database
echo -e "\nSetting up the database"
echo -e "\nExporting the database variables to bashrc"
sudo echo "" >> ~/.bashrc
sudo echo "export FLASK_APP='rbl_backend'" >> ~/.bashrc
sudo echo "export DATABASE_URL='postgresql://eplfcjzsjrlefx:bfd39aa631ea4971aa380f49dada5a6463a0439d0d977058cf7243b60610eae0@ec2-54-208-104-27.compute-1.amazonaws.com:5432/dbh4a6k8ork3tk'" >> ~/.bashrc
source ~/.bashrc

echo -e "\nCreating the database"
flask db init
flask db migrate
flask db upgrade

# Starting the redis server and postgresql server
echo -e "\nStarting the redis server"
sudo service redis-server start

echo -e "\nStarting the postgresql server"
sudo service postgresql start

echo -e "\nStarting the gunicorn server"
gunicorn --bind 0.0.0.0:8000 app:app