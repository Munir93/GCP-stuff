from extra_packages.infectious_media_hooks import GoogleCloudStorageHook, ImPandasProcessingHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.version import version



class DownloadGCSObjectOperator(BaseOperator):

	'''
	Downloads object from GCS bucket to a local file
	Bucket name must be included in object name. 
	Uses End User Auth from environment

	:params project: GCP project 
	:type project: STRING

	:params objects: URI of object stored in bucket. Must include bucket name without 
					 gs:// extension 
	:type objects : STRING 

	:params filename: name of file to be stored localy
	:type filename: STRING
	'''

	@apply_defaults
	def __init__(self, project, objects, filename, *args, **kwargs):

		super(DownloadGCSObjectOperator, self).__init__(*args, **kwargs)

		self.project = project
		self.objects = objects
		self.filename = filename


	def execute(self, context):

		hook = GoogleCloudStorageHook(project=self.project)

		hook.download_object(objects = self.objects, filename = self.filename)


class DownloadGCSObjectToDFOperator(BaseOperator):
	
	'''
	Downloads object from GCS bucket to pandas DF
	Bucket name must be included in object name. 
	Uses End User Auth from environment

	:params project: GCP project 
	:type project: STRING

	:params objects: URI of object stored in bucket. Must include bucket name without 
					 gs:// extension 
	:type objects : STRING 
	'''

	@apply_defaults
	def __init__(self, project, objects, *args, **kwargs):

		super(DownloadGCSObjectToDFOperator, self).__init__(*args, **kwargs)

		self.project = project
		self.objects = objects

	def execute(self, context):

		hook = GoogleCloudStorageHook(project = self.project)

		hook.download_object_todf(objects = self.objects)



class UpLoadGCSObjectOperator(BaseOperator):

	'''
	Uploads object to GCS bucket
	Bucket name must be included in object name. 
	Uses End User Auth from environment

	:params project: GCP project 
	:type project: STRING

	:params filename: path to file to be uploaded
	:type filename: STRING

	:params objects: URI of object stored in bucket. Must include bucket name without 
					 gs:// extension 
	:type objects : STRING 
	'''

	@apply_defaults
	def __init__(self, project, filename, objects, *args, **kwargs):

		super(UpLoadGCSObjectOperator, self).__init__(*args, **kwargs)

		self.project = project
		self.objects = objects
		self.filename = filename

	def execute(self, context):

		hook = GoogleCloudStorageHook(project=self.project)

		hook.upload_object(filename=self.filename, objects = self.objects)


class CleanMarketingDataOperator(BaseOperator):

	'''
	Takes in IM csv file removes Null values and converts types. 
	Creates a data cleansing report formatted 'data_cleansing_report<todays date>.txt

	:params input_csv: path to csv 
	:type input_csv: STRING 

	:params output_csv: name of output file. If None returns DF with cleaned data. 
	:type output_csv: STRING 
	'''

	@apply_defaults
	def __init__(self, input_csv, output_csv=None, *args, **kwargs):

		super(CleanMarketingDataOperator, self).__init__(*args, **kwargs)

		self.input_csv = input_csv
		self.output_csv = output_csv

	def execute(self, context):

		hook = ImPandasProcessingHook()

		hook.clean_csv_file(input_csv=self.input_csv, output_csv=self.output_csv)


class CalculateMarketingMetricsOperator(BaseOperator):

	'''
	Takes in cleaned IM csv file calculated various metrics. See Hook for details. 
	outputs csv.

	:params input_csv: path to csv 
	:type input_csv: STRING 

	:params output_csv: name of output file. If None returns DF with cleaned data. 
	:type output_csv: STRING 
	'''

	@apply_defaults
	def __init__(self, input_csv, output_csv=None, *args, **kwargs):

		super(CalculateMarketingMetricsOperator, self).__init__(*args, **kwargs)

		self.input_csv = input_csv
		self.output_csv = output_csv

	def execute(self, context):

		hook = ImPandasProcessingHook()

		hook.calculate_metrics(input_csv=self.input_csv, output_csv=self.output_csv)


class CalculateMarketingMetricsByWeekOperator(BaseOperator):

	'''
	Takes in cleaned IM csv file calculated various metrics and groups by week. See Hook for details. 
	outputs csv. 
	
	:params input_csv: path to csv 
	:type input_csv: STRING 

	:params output_csv: name of output file. If None returns DF with cleaned data. 
	:type output_csv: STRING 
	'''

	@apply_defaults
	def __init__(self, input_csv, output_csv=None, *args, **kwargs):

		super(CalculateMarketingMetricsByWeekOperator, self).__init__(*args, **kwargs)

		self.input_csv = input_csv
		self.output_csv = output_csv

	def execute(self, context):

		hook = ImPandasProcessingHook()

		hook.calculate_metrics_by_week(input_csv=self.input_csv, output_csv=self.output_csv)


