import logging
import apache_beam as beam
from apache_beam import Row
from apache_beam.options.pipeline_options import PipelineOptions
import argparse
from typing import List

from apache_beam.typehints import Tuple


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

dept = [
        {
            "id": "D101",
            "dept_name": "Support"
            },
        {
            "id": "D102",
            "dept_name": "HR"
            },
        {
            "id": "D103",
            "dept_name": "Marketing"
            },
        {
            "id": "D104",
            "dept_name": "Sells"
            }
        ]
def flat_join(element: tuple):
    data = element[1]
    dept = data["dept"][0]["dept_name"]
    return [ (dept,employee) for employee in data["employees"]]

def run():
    pipeline_options = PipelineOptions()
    with beam.Pipeline(options=pipeline_options) as p:

        employees_pcoll = p | 'Create employees' >> beam.Create(employees)
        dept_pcoll = p | 'Create department' >> beam.Create(dept)
        employees_kv = employees_pcoll | 'Add keys from dept employees' >> beam.WithKeys(lambda x: x["dept_id"])
        dept_kv = dept_pcoll | 'Add keys dept' >> beam.WithKeys(lambda x: x["id"])
        # 1 print a key example
        
        joined_cogroup = ({"employees": employees_kv,"dept": dept_kv}) | "Join with CoGroupByKey" >> beam.CoGroupByKey()
        # 2 print CoGroupByKey example
        flatten_joined = joined_cogroup | "Flatten ops" >> beam.FlatMap(flat_join)
        # 3 print join example with flatmap
        min_joined_date_by_dept = (flatten_joined | 
            "Group by dept" >> beam.GroupByKey() |
            "Take min of joining date" >> beam.Map(lambda x: 
                (x[0],min([employee["joining_date"] for employee in x[1]]))
            )
        )
        min_joined_date_by_dept_2 = (flatten_joined | 
            "Min of joining date Group by dept" >> beam.GroupBy(lambda x: x[0]).aggregate_field(
                lambda x: x[1]["joining_date"],min,"min_joining_date"
                )
        )

        # 4 print example groupbykey without aggregation after with aggregation and lastly group by key example

        min_joined_date_by_dept_2 | "Print pardo" >> beam.Map(print)
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
