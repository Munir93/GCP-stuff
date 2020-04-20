from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from extra_packages.infectious_media_operators import DownloadGCSObjectOperator, CleanMarketingDataOperator, CalculateMarketingMetricsOperator,CalculateMarketingMetricsByWeekOperator, UpLoadGCSObjectOperator
import datetime
from datetime import datetime as dt, date, timedelta
import json
from pathlib import Path

############## Dag Args #############

default_dag_args = {
    'start_date': datetime.datetime(2019, 7, 4),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=2),
    }

############# Reading from config file #############

with open('/Users/munir.welch/Desktop/Airflow_Project/im_config.json', 'rb') as c:
	config_buffer = c.read()
	config = json.loads(config_buffer)

#Extracting variables for operators
project = config["project"]
files_to_download = config["files"]


############# Start of DAG ################

dag = DAG('Munir_Welch_IM_Task_prod', default_args = default_dag_args, schedule_interval=None)

for file in files_to_download:

	date_name = file.split(' - ')[2].split('.')[0]
	# Start
	op1 = DummyOperator(task_id = 'Start', dag=dag)
	# Download file from GCS
	download_file = DownloadGCSObjectOperator(task_id='download_'+date_name, project=project, objects=file,
		filename='sample_data_'+date_name+'.csv', dag=dag)
	# Clean file 
	clean_file = CleanMarketingDataOperator(task_id = 'clean_file_'+date_name, input_csv='sample_data_'+date_name+'.csv',
	 output_csv ='cleaned.csv', dag=dag)
	# Calculate metrics
	calculate = CalculateMarketingMetricsOperator(task_id = 'calculate_metrics_'+date_name, input_csv='cleaned.csv',
	 output_csv='MW_IM_metrics_'+date_name+'.csv', dag=dag)
	# Calculate metrics by week
	calculate_week = CalculateMarketingMetricsByWeekOperator(task_id = 'calculate_weekly_'+date_name, input_csv='cleaned.csv',
		output_csv = 'MW_IM_metrics_by_week_'+date_name+'.csv', dag = dag)
	# Upload emtrics to GCS
	upload_metrics = UpLoadGCSObjectOperator(task_id = 'upload_metrics_'+date_name, project=project, filename = 'MW_IM_metrics_'+date_name+'.csv',
		objects = 'im_public_interview_task_data_eng_dest/MW_IM_metrics_'+date_name+'.csv', dag=dag)
	# Upload weekly to GCS
	upload_metrics_by_week = UpLoadGCSObjectOperator( task_id = 'upload_weekly_'+date_name, project=project, filename='MW_IM_metrics_by_week_'+date_name+'.csv',
		objects = 'im_public_interview_task_data_eng_dest/MW_IM_metrics_by_week_'+date_name+'.csv', dag=dag)
	# Upload cleansing report to GCS
	upload_report = UpLoadGCSObjectOperator(task_id = 'upload_report_'+date_name, project=project,filename = 'data_cleansing_report'+(date.today()).strftime("%Y%m%d")+'.txt', 
		objects='im_public_interview_task_data_eng_dest/MW_data_cleansing_reports/report_'+(date.today()).strftime("%Y%m%d")+'.txt', dag = dag)
	# End
	op2 = DummyOperator(task_id = 'End', dag=dag)

	op1 >> download_file >> clean_file >> calculate  >> upload_metrics >> op2
	clean_file >> calculate_week >> upload_metrics_by_week >> op2
	clean_file >> upload_report >> op2
	
