
from stream_logs import generate_log_line
from concurrent import futures
from typing import Callable
from google.cloud import pubsub_v1
import random
import time
import os

service_account_key = r"soulcode-331512-8fe205b6b6f8.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_key

PROJECT_ID = 'soulcode-331512'
TOPIC_ID = 'logs_topic'

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
publish_futures = []

def get_callback(
  publish_future: pubsub_v1.publisher.futures.Future, data: str
  ) -> Callable[[pubsub_v1.publisher.futures.Future], None]:
  
  def callback(publish_future: pubsub_v1.publisher.futures.Future) -> None:
    try:
      # Wait 60 seconds for the publish call to succeed.
      print(publish_future.result(timeout=60))
    except:
      print(f"Publishing {data} timed out")

  return callback

if __name__ == '__main__':
  
  while True:
    data = generate_log_line()
    print(data)
    # When you publish a message, the client returns a future.
    publish_future = publisher.publish(topic_path, data.encode('utf8'))
    # Non-blocking. Publish failures are handled in the callback function.
    publish_future.add_done_callback(get_callback(publish_future, data))
    publish_futures.append(publish_future)

    time_to_sleep = random.randint(1,5)
    time.sleep(time_to_sleep)

# Wait for all the publish futures to resolve before exiting
# futures.wait(publish_futures, return_when=futures.ALL_COMPLETED)


