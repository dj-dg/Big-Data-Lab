import random
import urllib
import requests
# from bs4 import BeautifulSoup
# import numpy
import re
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import shutil
import zipfile
import os
from datetime import datetime, timedelta


#First we initialize the default arguments and the DAG to be used here
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=50000),
}

dag = DAG(
    'data_pipeline',
    default_args=default_args,
    description='NOAA data processing pipeline',
    schedule_interval=timedelta(days=1),
)

#Define a function to get the URL
base_url  = "https://www.ncei.noaa.gov/data/local-climatological-data/access/"
def url_create(base_url, year):
    new_url = base_url + str(year) + '/'
    return new_url
year = 2020
new_url = url_create(base_url,year)

#The first task, using a Bash Operator to get the links of the files on the page
get_csv_links = BashOperator(
    task_id = 'get_csv_links',
    bash_command = f"curl -s {new_url} | grep -o -E 'href=\"([^\"]+)\"' | awk -F'\"' '{{print $2}}' > /root/airflow/logs/file_links.txt",
    dag=dag,
)

def select_random_files():
    file_count = 5
    downloads_list = []
    with open('/root/airflow/logs/file_links.txt', 'r') as files:
        for file in files:
            downloads_list.append(file.strip())
    file_links = random.sample(downloads_list,file_count)

    with open('/root/airflow/logs/selected_links.txt', 'w') as selected_link:
        for link in file_links:
            selected_link.write(link)
    
    return file_links

#The second task, using a Python function to randomly choose a number of files from the list obtained
random_file_select = PythonOperator(
    task_id = 'select_random_files',
    python_callable = select_random_files,
    op_args=[],
    dag=dag,
)


def get_csv_files():
    with open('/root/airflow/logs/selected_links.txt', 'r') as files:
        content = files.read()
    csv_file_list = []
    for filename in content.split(".csv"):
        if filename:
            csv_file_list.append(filename + '.csv') 

    directory = "/root/airflow/logs/archive/"

    if os.path.exists(directory):
        shutil.rmtree(directory)
    os.makedirs(directory)
    
    for link in csv_file_list:
        links = new_url + link
        os.system(f'wget -P {directory} {links}')

#The third task, using a Python function to get the respective names of the csv files and save them 
choose_csv_files = PythonOperator(
    task_id = 'choose_csv_files',
    python_callable = get_csv_files,
    op_args = [],
    dag=dag,
)

def zip_archive():
    archive_directory = "/root/airflow/logs/archive/"
    zip_directory = "/root/airflow/logs/"

    with zipfile.Zipfile(os.path.join(zip_directory,'archive.zip'),'w') as zip_file:
        for root, dirs, files in os.walk(archive_directory):
            for file in files:
                file_path = os.path.join(root, file)
                zip_file.write(file_path, os.path.relpath(file_path,archive_directory))


#The fourth task, using a Python function to put the downloaded files into an archive and compress the folder
archive_zip_files = PythonOperator(
    task_id = "archive_zip_files",
    python_callable = zip_archive,
    op_args=[],
    dag=dag,
)

def replace_archive():
    archive_directory = "/root/airflow/logs/archive/"

    if os.path.exists(archive_directory):
        shutil.rmtree(archive_directory)

#The fifth task, to place the archive at the required directory and to replace the existing archive if there already
check_archive = PythonOperator(
    task_id = "replace_archive",
    python_callable = replace_archive,
    op_args=[],
    dag=dag,
)

#We then create a DAG putting these 5 tasks in sequential order
get_csv_links >> random_file_select >> choose_csv_files >> archive_zip_files >> check_archive


