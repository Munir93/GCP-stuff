# IM Interview Task 

## ETL built using Apache Airflow 
<b> Summary </b>
- Downloads file from GCS
- Cleans file 
- Calculates metrics by day and week 
- Uploads to GCS

<b> Directory Summary </b>
- <b> im_config.json - </b> Config file containing parameters for DAG such as GCP project 
- <b> im_task </b> - Folder for source code 
- <b> infectious_media.py </b> - DAG file 
- <b> extra packages </b> - Folder for Hooks and Operators 
- <b> infectious_media_hooks.py </b> - custom hooks for DAG
- <b> infectious_media_operators.py</b> - custom operators for DAG



Munir Welch 7/7/19
