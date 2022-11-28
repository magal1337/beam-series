import logging
import apache_beam as beam
from apache_beam import Row
from apache_beam.options.pipeline_options import PipelineOptions
import argparse
from typing import List

def run():

    parser = argparse.ArgumentParser()

    parser.add_argument("--pcollSize",required=True)

    know_args, pipeline_args = parser.parse_known_args()
    # Integer types
    data_list = [x for x in range(10)]

    # String types

    data_list = ["banana","orange","apple"]

    # dict types
    data_list = [{"a":"banana","b":"apple"},{"a": "orange","b":"foo"}]

    # Row types

    data_list = [Row(name="fernando",age=13),Row(name="lucas",age=22)]

    # mix list

    data_list = [1,2,3,"banana",4]

    # dynamic integer types

    def generate_sequence(element: int)->List[int]:
        return [x for x in range(element)]
    

    pipeline_options = PipelineOptions(pipeline_args)
    with beam.Pipeline(options=pipeline_options) as p:

        data_list = generate_sequence(int(know_args.pcollSize))
        pcoll = p | 'Create Data List' >> beam.Create(data_list).with_output_types(int)

        logging.info(type(pcoll))

        pcoll | 'print Collection' >> beam.Map(lambda x: print(x))

        p.run()




if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
