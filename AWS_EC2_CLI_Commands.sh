#Connect to EC2 Instance via VS Code

#Run the following series of commands in the terminal 
sudo apt update
sudo apt install python3-pip
sudo apt install python3.10-venv
python3 -m venv redfin_venv
source redfin_venv/bin/activate
pip install pandas
pip install boto3
pip install --upgrade awscli
pip install "apache-airflow[celery]==2.8.1" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.1/constraints-3.10.txt"
aws configure
airflow standalone
