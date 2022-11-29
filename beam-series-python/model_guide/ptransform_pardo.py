import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

accounts = [
        {
            "name": "Lucas",
            "age": 32,
            "gender": "M",
            "email": "lucas@foo.com",
            "is_active": True
            },
        {
            "name": "Fernando",
            "age": 38,
            "gender": "M",
            "email": "fernando@foo.com",
            "is_active": True
            },
        {
            "name": "Maria",
            "age": 14,
            "gender": "F",
            "email": "maria@foo.com",
            "is_active": False
            },
        ]

def age_to_str(account: dict):
    account["age"] = str(account["age"])
    return account

class MyDoFn(beam.DoFn):

    def start_bundle(self):
        print("iniciando bundle")
    def process(self,element):
        if element["is_active"]:
            element["status"] = "approved"
        else:
            element["status"] = "rejected"
        yield element
    def finish_bundle(self):
        print("finalizando bundle")
def run():
    pipeline_options = PipelineOptions()
    with beam.Pipeline(options=pipeline_options) as p:

        pcoll = p | 'Create pcoll' >> beam.Create(accounts)

        beam_filter = pcoll | "Filter aged" >> beam.Filter(lambda account: account["age"] >= 18)
        beam_format = pcoll | "Format age" >> beam.Map(lambda account: age_to_str(account))
        beam_extract = pcoll | "extract email" >> beam.Map(lambda account: {"email": account["email"]})
        beam_custom = pcoll | "pardo" >> beam.ParDo(MyDoFn())
        #beam_filter | "Print filter" >> beam.Map(print)
        #beam_format | "Print format" >> beam.Map(print)
        #beam_extract | "Print email" >> beam.Map(print)
        beam_custom | "Print pardo" >> beam.Map(print)
        p.run()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
