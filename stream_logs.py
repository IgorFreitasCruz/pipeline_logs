from faker import Faker
import random
import numpy as np
from datetime import datetime

LINE = """\
{remote_address} - - [{time_local}] "{request_type} {request_path} HHTP/1.1" [{status}] {body_bytes_sent} "{http_referer}" "{http_user_agent}"\
"""

def generate_log_line():
  """
  define fake fields for the logs
  """
  fake = Faker()
  now = datetime.now()
  remote_address = fake.ipv4()
  time_local = now.strftime("%d/%b/%Y:%H:%M:%S")
  request_type = random.choice(["GET", "POST", "DELETE",])
  request_path = "/" + fake.uri_path()

  status = np.random.choice([200, 401, 404], p = [0.9, 0.05, 0.05])
  body_bytes_sent = random.choice(range(5,1000,1))
  http_referer = fake.uri()
  http_user_agent = fake.user_agent()

  log_line = LINE.format(
    remote_address = remote_address,
    time_local = time_local,
    request_type = request_type,
    request_path = request_path,
    status = status,
    body_bytes_sent = body_bytes_sent,
    http_referer = http_referer,
    http_user_agent = http_user_agent
  )

  return log_line


