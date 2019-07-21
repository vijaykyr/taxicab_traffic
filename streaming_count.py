"""A streaming dataflow pipeline to count pub/sub messages.
"""

from __future__ import absolute_import

import argparse
import logging
from datetime import datetime

from past.builtins import unicode

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.examples.wordcount import WordExtractingDoFn
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.io import WriteToText

class CountFn(beam.CombineFn):
  def create_accumulator(self):
    return 0

  def add_input(self, count, input):
    return count + 1

  def merge_accumulators(self, accumulators):
    return sum(accumulators)

  def extract_output(self, count):
    return count

def run(argv=None):
  """Build and run the pipeline."""
  parser = argparse.ArgumentParser()
  

  parser.add_argument(
      '--project',
      help=('Google Cloud Project ID'),
      required=True)  
  parser.add_argument(
      '--input_topic',
      help=('Google Cloug PubSub topic name '),
      required=True)

  known_args, pipeline_args = parser.parse_known_args(argv)
  print('Pipeline args: {}'.format(pipeline_args))
  print('Known args: {}'.format(known_args))

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(
    pipeline_args.append('--project={}'.format(known_args.project)))
  pipeline_options.view_as(SetupOptions).save_main_session = True
  pipeline_options.view_as(StandardOptions).streaming = True
  p = beam.Pipeline(options=pipeline_options)

  # Read from PubSub into a PCollection.
  TOPIC = 'projects/{}/topics/{}'.format(known_args.project,known_args.input_topic)
  messages = p | beam.io.ReadFromPubSub(topic=TOPIC).with_output_types(bytes)

  windows = messages | beam.WindowInto(window.FixedWindows(size=15))

  count = windows | 'count' >> beam.CombineGlobally(CountFn()).without_defaults()

  def to_dict(count):
    return {'rides_last_10_min':count,'time':datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

  count_dict = count | 'to_dict' >> beam.Map(to_dict)

  # dataset_id.table_id (dataset needs to exist, but table will be created)
  table_spec = '{}:taxifare.traffic'.format(known_args.project)
  
  count_dict | 'write_bq' >> beam.io.WriteToBigQuery(
    table_spec,
    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND, #WRITE_TRUNCATE not supported for streaming
    create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER) #CREATE_IF_NEEDED didn't work, kept sleeping after create


  result = p.run()
  #result.wait_until_finish() #only do this if running with DirectRunner


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()