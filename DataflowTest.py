import argparse
import json
import logging

import apache_beam as beam
import pandas as pd
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import storage
from smart_open import open


class ReadFile(beam.DoFn):

    def __init__(self, input_path):
        self.input_path = input_path

    def start_bundle(self):
        from google.cloud import storage
        self.client = storage.Client()

    def process(self, something):
        clear_data = []
        with open(self.input_path) as fin:
            for line in fin:
                data = json.loads(line)
                product = data.get('product')

                if product and product.get('id'):
                    product_id = str(product.get('id'))
                    vendor = product.get('vendor')
                    product_type = product.get('product_type')
                    updated_at = product.get('updated_at')
                    created_at = product.get('created_at')
                    product_options = product.get('options')
                    
                    clear_data.append([product_id, vendor, product_type, updated_at, created_at])

        yield clear_data


class WriteCSVFIle(beam.DoFn):

    def __init__(self, bucket_name):
        self.bucket_name = bucket_name

    def start_bundle(self):
        from google.cloud import storage
        self.client = storage.Client()

    def process(self, mylist):
        df = pd.DataFrame(mylist, columns={'product_id': str, 'vendor': str, 'product_type': str, 'updated_at': str, 'created_at': str})

        bucket = self.client.get_bucket(self.bucket_name)
        bucket.blob(f"csv_exports.csv").upload_from_string(df.to_csv(index=False), 'text/csv')


class DataflowOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--input_path', type=str, default='gs://new-buclet-8022/input.json')
        parser.add_argument('--output_bucket', type=str, default='gs://new-buclet-8022')


def run(argv=None):
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    dataflow_options = pipeline_options.view_as(DataflowOptions)

    with beam.Pipeline(options=pipeline_options) as pipeline:
        (pipeline
         | 'Start' >> beam.Create([None])
         | 'Read JSON' >> beam.ParDo(ReadFile(dataflow_options.input_path))
         | 'Write CSV' >> beam.ParDo(WriteCSVFIle(dataflow_options.output_bucket))
         )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
