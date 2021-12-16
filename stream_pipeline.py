from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import pubsub_v1
from google.cloud import bigquery
import apache_beam as beam
import os
import argparse
import logging
import re

service_account_key = r"soulcode-331512-8fe205b6b6f8.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_key

PROJECT_ID = 'soulcode-331512'
schema = """remote_address:STRING,
            timelocal:TIMESTAMP,
            request_type:STRING,
            body_bytes_sent:STRING,
            status:STRING,
            http_referer:STRING,
            http_user_agent:STRING"""
TOPIC = 'projects/soulcode-331512/topics/logs_topic'


def clean_logs(data):

	PATTERNS = [r'(^\S+\.[\S+\.]+\S+)\s', r'(?<=\[).+?(?=\])',
							r'\"(\S+)\s(\S+)\s*(\S*)\"', r'\s(\d+)\s', r"(?<=\[).\d+(?=\])",
							r'\"[A-Z][a-z]+', r'\"(http|https)://[a-z]+.[a-z]+.[a-z]+']

	result = []
	for match in PATTERNS:
			try:
					reg_match = re.search(match, data).group()
					if reg_match:
							result.append(reg_match)
					else:
							result.append(" ")
			except:
					print("There was as error with the regex search")

	result = [x.strip() for x in result]
	result = [x.replace('"', "") for x in result]
	res = ','.join(result)

	return res


class Split(beam.DoFn):

	def process(self, element):
			from datetime import datetime
			element = element.split(',')
			d = datetime.strptime(element[1], "%d/%b/%Y:%H:%M:%S")
			date_string = d.strftime('%Y-%m-%d %H:%M:%S')

			return [{
					'remote_address': element[0],
					'timelocal': date_string,
					'request_type':element[2],
					'status':element[3],
					'body_bytes_sent':element[4],
					'http_referer':element[5],
					'http_user_agent':element[6]
			}]


def main():

	# parser = argparse.ArgumentParser()
	# parser.add_argument("--input_topic")
	# parser.add_argument("--output")
	# known_args = parser.parse_known_args(argv)

	pipeline_options = {
    'project': 'soulcode-331512' ,
    'runner': 'DataflowRunner',
    'region': 'southamerica-east1',
    'staging_location': 'gs://data_ingestion_soulcode/staging',
    'temp_location': 'gs://data_ingestion_soulcode/temp',
    # 'template_location': 'gs://data-lake-soul-gcp/template/stream_voos',
    # 'save_main_session': True ,
    'streaming' : True }

	pipeline_options = PipelineOptions.from_dictionary(pipeline_options)

	p = beam.Pipeline(options=pipeline_options)

	(p
		| 'ReadData' >> beam.io.ReadFromPubSub(topic=TOPIC).with_output_types(bytes)
		| "Decode" >> beam.Map(lambda x: x.decode('utf-8'))
		| "Clean Data" >> beam.Map(clean_logs)
		| 'ParseCSV' >> beam.ParDo(Split())
		| 'WriteToBigQuery' >> beam.io.WriteToBigQuery('{0}:userlogs.logdata'.format(PROJECT_ID),
																									schema=schema,
																									create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
																									write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)
		)

	result = p.run()
	result.wait_until_finish()


if __name__ == '__main__':
    logger = logging.getLogger().setLevel(logging.INFO)
    main()
