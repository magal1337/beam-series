import logging
import random
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

employees = [
        {
            "id": 0,
            "name": "John C",
            "joining_date": "2000-01-01",
            "dept_id": "D101",
            "is_active": True
            },
        {
            "id": 1,
            "name": "Tom D",
            "joining_date": "2002-02-01",
            "dept_id": "D102",
            "is_active": True
            },
        {
            "id": 2,
            "name": "Max",
            "joining_date": "2003-04-01",
            "dept_id": "D104",
            "is_active": False
            },
        {
            "id": 3,
            "name": "Bruce",
            "joining_date": "2003-05-01",
            "dept_id": "D102",
            "is_active": False
            },
        {
            "id": 4,
            "name": "Barry",
            "joining_date": "2003-04-22",
            "dept_id": "D101",
            "is_active": False
            },
        {
            "id": 5,
            "name": "Clark",
            "joining_date": "2001-04-01",
            "dept_id": "D103",
            "is_active": False
            },
        {
            "id": 6,
            "name": "Bryan C.",
            "joining_date": "2010-07-01",
            "dept_id": "D104",
            "is_active": True
            }
        ]
def last_joining_date(values):
    return max(values)

def partition_fn(employee, num_partitions):
    return random.randrange(num_partitions)

class LastDateFn(beam.CombineFn):
    def create_accumulator(self):
        return "0000-00-00"
    def add_input(self, acc, input):
        return max(acc,input["joining_date"])
    def merge_accumulators(self, acc):
        return max(acc)
    def extract_output(self, acc):
        return acc

def run():
    pipeline_options = PipelineOptions()
    with beam.Pipeline(options=pipeline_options) as p:
    # pre-built combine, simple combine and complex combine, partition and flatten example
        employees_pcoll = p | 'Create employees' >> beam.Create(employees) 
        # combine globally
        employees_id = employees_pcoll | 'Get Id' >> beam.Map(lambda x: x["id"])
        max_id_employee = employees_id | "Max ID Globally" >> beam.CombineGlobally(max)
        # KV Pcollection
        employees_kv = employees_pcoll | 'Add keys dept' >> beam.WithKeys(lambda x: x["dept_id"])
        # Simple Pre-built combine
        employees_id_kv = employees_kv | 'Extract only ID' >> beam.Map(lambda x: (x[0],x[1]["id"]))
        max_id_by_dept = employees_id_kv | 'Max ID per Dept' >> beam.CombinePerKey(max)
        # Simple CombineFn
        max_joining_date_by_dept = employees_id_kv | 'Max Joining Date per Dept' >> beam.CombinePerKey(last_joining_date)
        max_joining_date_by_dept_complex = employees_kv | 'Max Joining Date per Dept Complex' >> beam.CombinePerKey(LastDateFn())

        pcoll_groups = employees_pcoll | "Partition Random" >> beam.Partition(partition_fn,3)
        pcoll_union = pcoll_groups | "Union Pcollection" >> beam.Flatten() 
        pcoll_union | "Print pardo" >> beam.Map(print)
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
