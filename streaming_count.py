"""A streaming dataflow pipeline to count pub/sub messages.
"""

from __future__ import absolute_import

import argparse
import logging

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
  
  group = parser.add_mutually_exclusive_group(required=True)
  
  group.add_argument(
      '--input_topic',
      help=('Input PubSub topic of the form '
            '"projects/<PROJECT>/topics/<TOPIC>".'))
  group.add_argument(
      '--input_subscription',
      help=('Input PubSub subscription of the form '
            '"projects/<PROJECT>/subscriptions/<SUBSCRIPTION>."'))
  known_args, pipeline_args = parser.parse_known_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  pipeline_options.view_as(StandardOptions).streaming = True
  p = beam.Pipeline(options=pipeline_options)

  # Read from PubSub into a PCollection.
  if known_args.input_subscription:
    messages = (p
                | beam.io.ReadFromPubSub(
                    subscription=known_args.input_subscription)
                .with_output_types(bytes))
  else:
    messages = (p
                | beam.io.ReadFromPubSub(topic=known_args.input_topic)
                .with_output_types(bytes))
  # Read from PubSub into a PCollection.


  windows = messages | beam.WindowInto(window.FixedWindows(size=15))


  #windows | 'write' >> WriteToText('raw.txt')

  count = windows | 'count' >> beam.CombineGlobally(CountFn()).without_defaults()

  count | 'write2' >> WriteToText('count.txt')

  result = p.run()
  result.wait_until_finish()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()