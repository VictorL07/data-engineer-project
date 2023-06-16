"""
Steps:
1. Read a Bigquery Table
2. Split all records by specific threshold
3. Parse records
4. Send records by PubSub
"""

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import ReadFromBigQuery
from apache_beam import ParDo, Pipeline
from apache_beam.io.gcp.pubsub import WriteToPubSub
import json

class AddKeyFn(beam.DoFn):
    def __init__(self, batch_size):
        self.batch_size = batch_size
        self.counter = 0

    def process(self, element):
        import apache_beam as beam
        key = self.counter // self.batch_size
        self.counter += 1
        if key == 0:
            yield beam.pvalue.TaggedOutput("first_data", element)
        elif key == 1:
            yield beam.pvalue.TaggedOutput("second_data", element)

def run():
    options = PipelineOptions()
    query = 'SELECT column1, column2, column3, column4 FROM `project.dataset.table_name`'
    threshold = 500

    with Pipeline(options=options) as pipeline:
        
        element = pipeline | "Read from BigQuery" >> ReadFromBigQuery(query=query, use_standard_sql=True)

        element_key =  element | "Add Key" >> ParDo(AddKeyFn(batch_size=threshold)).with_outputs("first_data", "second_data")

        first_group = (
            element_key.first_data 
                | 'Parsear como cadenas 1' >> beam.Map(lambda elem: json.dumps(elem).encode())
                | "Write to PubSub 1" >> WriteToPubSub(topic='projects/project/topics/topic-name-0b2e5ed7c7a2', with_attributes=False)
            )

        second_group = (
            element_key.second_data 
                | 'Parsear como cadenas 2' >> beam.Map(lambda elem: json.dumps(elem).encode())
                | "Write to PubSub 2" >> WriteToPubSub(topic='projects/project/topics/topic-name-cdf5ffd24da8', with_attributes=False)
            )

if __name__ == '__main__':
    run()