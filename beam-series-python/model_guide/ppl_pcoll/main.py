import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import argparse
from apache_beam import Row

def run():

    parser = argparse.ArgumentParser()
    parser.add_argument("--runner",required=False,default="DirectRunner")
    parser.add_argument("--pcollSize",required=True)
    know_args,pipeline_args = parser.parse_known_args()
    # Integer values
    data_list = [x for x in range(10)]
    # String values
    data_list2 = ["banana","apple","orange"]
    # dict values
    data_list = [{"a": "banana"},{"b": "apple"},{"c": "orange"}]

    # Row type

    data_list = [Row(name="Fernando",age=14),Row(name="Lucasd",age=33)]

    # element type (this case will work without coder implicits)
    #data_list = [1,2,3,4,33,4]

    #data_list = [x for x in range(int(know_args.pcollSize))]

    pipeline_options = PipelineOptions(pipeline_args)
    with beam.Pipeline(options=pipeline_options) as p:
        
        pcoll = p | 'Create Data List' >> beam.Create(data_list)   #.with_output_types(str)
        
        logging.info(type(pcoll))
        pcoll | 'print Collection' >> beam.Map(lambda x: print(x.name))
        p.run()



if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
