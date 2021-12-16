import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import logging
import re

PROJECT_ID = 'soulcode-331512'
schema = """remote_addr:STRING,
            timelocal:STRING,
            request_type:STRING,
            status:STRING,
            body_bytes_sent:STRING,
            http_referer:STRING,
            http_user_agent:STRING"""

src_path = "user_log_fileC.txt"


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
	p = beam.Pipeline(options=PipelineOptions())
	
	(p| 'ReadData' >> beam.io.textio.ReadFromText(src_path)
		| 'CleanLogs' >> beam.Map(clean_logs)
		| 'ParseCSV' >> beam.ParDo(Split())
		| 'WriteToBigQuery' >> beam.io.WriteToBigQuery('{0}:userlogs.logdata'.format(PROJECT_ID),
		schema=schema,
		write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
		create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
	
	p.run()
  
if __name__ == '__main__':
  logger = logging.getLogger().setLevel(logging.INFO)
  main()
