
import logging
import apache_beam as beam
from apache_beam import Row
from apache_beam.options.pipeline_options import PipelineOptions
import argparse
from typing import List

accounts = [
        {
            "name": "Lucas",
            "age": 32,
            "gender": "M",
            "email" : "lucas@foo.com",
            "is_active": True
            },
        {
            "name": "Fernando",
            "age": 35,
            "gender": "M",
            "email" : "fernando@foo.com",
            "is_active": True
            },
        {
            "name": "Maria",
            "age": 14,
            "gender": "F",
            "email" : "maria@foo.com",
            "is_active": False
            }
        ]

def age_to_str(account: dict)->dict:
    account["age"] = str(account["age"])
    return account

class MyDoFn(beam.DoFn):
    def start_bundle(self):
        print("comecando um novo bundle")
    def process(self,element):
        if element["is_active"]:
            element["status"] = "approved"
        else:
            element["status"] = "rejected"
        yield element
    def finish_bundle(self):
        print("terminando um bundle")
def run():


    pipeline_options = PipelineOptions()
    with beam.Pipeline(options=pipeline_options) as p:
        
        pcoll = p | 'Create Pcoll' >> beam.Create(accounts)

        beam_filter = pcoll | 'Filter aged' >> beam.Filter(lambda account: account["age"] > 30)
        beam_format = pcoll | 'Format age to string' >> beam.Map(lambda account: age_to_str(account))
        beam_extract = pcoll | 'Extract email' >> beam.Map(lambda account: {"email": account["email"]})
        beam_pardo = pcoll | 'Add status' >> beam.ParDo(MyDoFn()) 
        #beam_filter | "Print Result" >> beam.Map(print)

        #beam_format | "Print format" >> beam.Map(print)

        #beam_extract | "Print extract" >> beam.Map(print)

        beam_pardo | "Print ParDo" >> beam.Map(print)
        p.run()




if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
