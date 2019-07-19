import time
import gzip
import logging
import argparse
import datetime
from google.cloud import pubsub

TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
TOPIC = 'sandiego'

if __name__ == '__main__':
   parser = argparse.ArgumentParser(description='Send sensor data to Cloud Pub/Sub in small groups, simulating real-time behavior')
   parser.add_argument('--project', help='Example: --project $DEVSHELL_PROJECT_ID', required=True)
   args = parser.parse_args()

   # create Pub/Sub notification topic
   logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
   publisher = pubsub.PublisherClient()
   topic_name = publisher.topic_path(args.project,TOPIC)
   try:
      publisher.get_topic(topic_name)
      logging.info('Reusing pub/sub topic {}'.format(TOPIC))
   except:
      publisher.create_topic(topic_name)
      logging.info('Creating pub/sub topic {}'.format(TOPIC))

   while True:
        publisher.publish(topic_name,'taxi_ride')
        logging.info('Publishing: {}'.format(time.ctime()))
        time.sleep(5)