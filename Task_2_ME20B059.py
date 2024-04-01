import random
import urllib
import requests
# from bs4 import BeautifulSoup
import numpy as np
import re
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.filesystem import FileSensor
import shutil
import zipfile
import os
from datetime import datetime, timedelta
import pandas as pd
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam.runners.interactive.interactive_beam as ib

#First we initialize the default arguments and the DAG to be used here
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=50000),
}

dag = DAG(
    'image_pipeline',
    default_args=default_args,
    description='Image generation pipeline',
    schedule_interval=timedelta(days=1),
)

extract_directory = "/root/airflow/logs/archive/"
output_directory = "/root/airflow/logs/"

#For the first task, we need to check the availability of the archive for extraction using the FileSensor operator
check_available_file = FileSensor(
    task_id  = "check_file_status",
    filepath = "/root/airflow/logs/archive.zip",
    timeout=5,
    op_args=[],
    dag=dag,
)

#For the second task, we need to unzip the archive to the location specified
open_archive = BashOperator(
    task_id = "unzip_archive",
    bash_command = "unzip -d /root/airflow/logs/archive/  /root/airflow/logs/archive.zip",
    op_args=[],
    dag=dag,
)

def data_manip():
    def format_csv(csv_file):
        csv_file_path = os.path.join(extract_directory,csv_file)
        df = pd.read_csv(csv_file_path)
        fields = ['HourlyWindSpeed', 'HourlyDryBulbTemperature'] #Can add more fields as needed
        df = df.dropna(subset=fields)
        df_chosen = df[["DATE"] + fields]
        latitude_longitude = (df["LATITUDE"].iloc[0], df["LONGITUDE"].iloc[0])
        return latitude_longitude, df_chosen.to_list()
    
    with beam.Pipeline(options=PipelineOptions()) as pipeline:
        collection=(
            pipeline
            | "Read .csv files" >> beam.Create(os.listdir(extract_directory))
            | "Format .csv file" >> beam.Map(format_csv)
            | "Write to output" >> beam.io.WriteToText(os.path.join(output_directory,'output.txt'))
        )

#For the third task, we extract the relevant data from the .csv files downloaded
data_extraction = PythonOperator(
    task_id = "data_extraction",
    python_callable = data_manip,
    op_args=[],
    dag=dag,
)    


def compute_monthly_averages():

    output_path = "/root/airflow/logs/averages.txt"

    def read_text_file(file_name):
        with open(file_name,'r') as file:
            lines = file.readlines()
            lines_list = []
            for line in lines:
                lines_list.append(line.strip())
            return lines_list

    def averages(line):
        data_tuple = eval(line.strip())
        latitude_longitude = data_tuple[0]
        all_data = data_tuple[1]

        monthly_averages = {}
        for data_point in all_data:
            date, wind_speed, bulb_temp = data_point
            month = datetime.strptime(date, "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m")

            if month not in monthly_averages:
                monthly_averages[month] = {"AverageWindSpeed": [], "AverageBulbTemperature": []}
            
            if not pd.isna(wind_speed):
                monthly_averages[month]["AverageWindSpeed"].append(wind_speed)
            
            if not pd.isna(bulb_temp):
                if not isinstance(bulb_temp, int):
                    bulb_temp = (bulb_temp[: len(bulb_temp)-1] if bulb_temp[-1] == 's' else bulb_temp)
                    monthly_averages[month]["AverageBulbTemperature"].append(float(bulb_temp))
            
        for month in monthly_averages.keys():
            monthly_averages[month]["AverageWindSpeed"] = sum(monthly_averages[month]["AverageWindSpeed"])/len(monthly_averages[month]["AverageWindSpeed"])
            monthly_averages[month]["AverageBulbTemperature"] = sum(monthly_averages[month]["AverageBulbTemperature"])/len(monthly_averages[month]["AverageBulbTemperature"])

        return latitude_longitude, monthly_averages
    

    with beam.Pipeline() as pipeline:
        collection = (
            pipeline
            | "Read from text file" >> beam.Create(read_text_file("/root/airflow/logs/result.txt-00000-of-00001"))
            | "Calculate averages" >> beam.Map(averages)
            | "Write to output file" >> beam.io.WriteToText(output_path)
        )

#For the fourth task, we calculate the averages of the parameters required from the dataframe
average_calculator = PythonOperator(
    task_id = "compute_monthly_averages",
    python_callable = compute_monthly_averages,
    op_args = [],
    dag=dag,
)


def delete_file():
    file_path = "/root/airflow/logs/archive/"
    if os.path.exists(file_path):
        os.remove(file_path)
    else:
        print("File does not exist")


#For the seventh task, we delete the file that was created
delete_csv_file = PythonOperator(
    task_id = 'delete_csv_file',
    python_callable = delete_file,
    op_args = [],
    dag=dag,
)

#Here we put all the tasks together in sequential order and create the DAG
check_available_file >> open_archive >> data_extraction >> average_calculator >> delete_csv_file


    




