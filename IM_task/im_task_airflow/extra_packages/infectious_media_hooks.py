from airflow.exceptions import AirflowException
import pandas as pd
import numpy as np
import gcsfs
import logging
from datetime import datetime as date, timedelta


class GoogleCloudStorageHook():

	'''
	Custom Hook for interacting with google cloud storage

	Uses End User Authentication inherited from local environment 

	'''

	def __init__(self, project):

		self.project = project


	def _create_filesystem(self):

		print('Creating filesystem for project %s', self.project)
		return gcsfs.GCSFileSystem(project= self.project, token = None)


	def download_object(self, objects, filename):
		
		fs = self._create_filesystem()

		if 'gs://' in objects:

			raise AirflowException('Please ensure the gs:// prefix is not included in object names')

		try:

			fs.get(rpath = objects, lpath=filename)

		except FileNotFoundError as e:

			print('The file %s was not found. Please check the spelling and try again', filename)
			raise AirflowException('There was an error, %s', e)

		return filename


	def upload_object(self, filename, objects):

		fs = self._create_filesystem()

		if 'gs://' in objects:

			raise AirflowException('Please ensure the gs:// prefix is not included in object names')

		try:
			fs.put(lpath=filename, rpath = objects)
		
		except FileNotFoundError as e:

			print('The file %s was not found. Please check the spelling and try again', filename)
			raise AirflowException('There was an error, %s', e)

		return objects

	# Not In Use
	def download_object_todf(self, objects):

		fs = self._create_filesystem()

		if 'gs://' not in objects:

			raise AirflowException('Please ensure the gs:// prefix is included in object names')

		try:

			with fs.open(objects, 'rb') as f:
				df = pd.read_csv(f)

		except FileNotFoundError as e:

			print('The file %s was not found. Please check the spelling and try again', filename)
			raise AirflowException('There was an error, %s', e)

		return df

class ImPandasProcessingHook():

	'''
	Custom Hook for cleaning and processing interview task file. 

	'''


	def clean_csv_file(self, input_csv, output_csv = None):

		log_lines = []
		time = ('Found %s. Starting file clean -- %s ' % (input_csv, (date.today()).strftime("%Y-%m-%d : %H:%M:%S")))
		log_lines.append(time)

		if isinstance(input_csv, pd.core.frame.DataFrame):

			df = input_csv

		else:

			try: 

				df = pd.read_csv(input_csv)

			except FileNotFoundError as e:

				print('File with name %s was not found. Please check spelling or path' % input_csv)
				raise AirflowException('There was an error please check the filename on the input csv', e)

		total_rows = df.shape[0]
		line = ('Found %s rows in this csv file. Searching for Nulls' % total_rows )
		log_lines.append(line)
		# Count of nulls 
		nulls = df.isnull().sum().sum()
		line = ('Found %s Null values' % nulls)
		log_lines.append(line)
		# Drop date column then try to convert all columns to numeric and rest to null
		cols = df.columns.drop('Creative Date')
		df[cols] = df[cols].apply(pd.to_numeric, errors='coerce')
		# Convert date column to datetime ror null
		df['Creative Date'] = pd.to_datetime(df['Creative Date'], errors='coerce', format='%d/%m/%Y')
		# count of missmatch data
		missmatch = df.isnull().sum().sum() - nulls

		line = ('Found %s invalid values' % missmatch)
		log_lines.append(line)
		line = ('Cleaning csv ...')
		log_lines.append(line)
		# drop all nulls
		df.dropna(inplace=True)
		# count final nulls
		cleaned = df.isnull().sum().sum()

		line = ('Cleaned! csv now contains %s null and invalid rows --  %s'  % (cleaned, (date.today()).strftime("%Y-%m-%d : %H:%M:%S")))
		log_lines.append(line)

		final_rows = df.shape[0]
		line = ('Final csv contains %s rows' % final_rows)
		log_lines.append(line)

		if missmatch or nulls > 0:
			line = 'Since bad data was found please check source system'
			log_lines.append(line)

		with open('data_cleansing_report'+(date.today()).strftime("%Y%m%d")+'.txt', 'w') as f:

			for line in log_lines:
				f.write(str(line)+'\n')
			f.close()

		#Converting date back to correct format
		df['Creative Date'] = df['Creative Date'].apply(lambda x : x.strftime('%d/%m/%Y'))

		if output_csv is not None:

			df.to_csv(output_csv, index=False)

			return output_csv

		return df


	def calculate_metrics(self, input_csv, output_csv = None):

		if isinstance(input_csv, pd.core.frame.DataFrame):

			df = input_csv

		else:

			try: 

				df = pd.read_csv(input_csv)

			except FileNotFoundError as e:

				print('File with name %s was not found. Please check spelling or path' % input_csv)
				raise AirflowException('There was an error please check the filename on the input csv', e)

		print('Calculating metrics for %s' % input_csv)

		# Calcualte Clicks
		df['Clicks'] = ((df['CTR (%)'] / 100) * df['Creative Impressions']).round()
		# Clculate Spend
		df['Spend (£)'] = ((df['CPM (£)'] * df['Creative Impressions']) / 1000).round(2)
		# Calculate Conversions 
		df['Conversions'] = (df['Spend (£)'] / df['CPConv (£)']).replace([np.inf, -np.inf], 0).round()
		# Convert empty cells to 0
		df['Conversions'] = df['Conversions'].convert_objects(convert_numeric = True).fillna(0)

		if output_csv is not None:

			df.to_csv(output_csv, index=False)

			return output_csv

		return df


	def calculate_metrics_by_week(self, input_csv, output_csv = None):

		if isinstance(input_csv, pd.core.frame.DataFrame):

			df = input_csv

		else:

			try: 

				df = pd.read_csv(input_csv)

			except FileNotFoundError as e:

				print('File with name %s was not found. Please check spelling or path' % input_csv)
				raise AirflowException('There was an error please check the filename on the input csv', e)
		# Calcualte Clicks
		df['Clicks'] = ((df['CTR (%)'] / 100) * df['Creative Impressions']).round()
		# Clculate Spend
		df['Spend (£)'] = ((df['CPM (£)'] * df['Creative Impressions']) / 1000).round(2)
		# Calculate Conversions 
		df['Conversions'] = (df['Spend (£)'] / df['CPConv (£)']).replace([np.inf, -np.inf], 0).round()
		# Convert empty cells to 0
		df['Conversions'] = df['Conversions'].convert_objects(convert_numeric = True).fillna(0)
		# Convert Date to datetime for grouby
		df['Creative Date'] = pd.to_datetime(df['Creative Date'], errors='coerce', format='%d/%m/%Y')
		# Dict for aggregates 
		d = {'Clicks' : ['sum'], 'Spend (£)' : ['sum'], 'Conversions' : ['sum'] }
		# Group by weeok 
		final = df.groupby(df['Creative Date'].dt.strftime('%W')).agg(d)
		# Round Spend
		final['Spend (£)'] = final['Spend (£)'].astype(float).round(2)

		if output_csv is not None:

			final.to_csv(output_csv, index=True)

			return output_csv

		return final


