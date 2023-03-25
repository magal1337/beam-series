import logging
import apache_beam as beam
from apache_beam import Row, coders, pvalue
from apache_beam.options.pipeline_options import List, PipelineOptions
import typing

from apache_beam.transforms.combiners import  MeanCombineFn

class Account(typing.NamedTuple):
    name: str
    age: int
    gender: str
    email: str

class Scores(typing.NamedTuple):
    account: str
    date: str
    score: int

coders.registry.register_coder(Account, coders.RowCoder)
coders.registry.register_coder(Scores, coders.RowCoder)

def to_account(record:str)->Account:
    entities = record.split(sep=",")
    return Account(
            name=entities[0],
            age=int(entities[1]),
            gender=entities[2],
            email=entities[3]
    )

def to_scores(record:str)->Scores:
    entities = record.split(",")
    return Scores(
            account=entities[0],
            date=entities[1],
            score=int(entities[2])
            )

def broadcast_join(score_entity: Scores, broad_table: List[Account]):
    account_name = [account.name for account in broad_table if account.email == score_entity.account][0]
    return Row(account_name=account_name,**score_entity._asdict())

    
def run():
    pipeline_options = PipelineOptions()
    with beam.Pipeline(options=pipeline_options) as p:

        pcoll_accounts = p | 'Create pcoll accounts' >> beam.io.ReadFromText("./data/accounts.csv",skip_header_lines=1)
        pcoll_scores = p | 'Create pcoll scores' >> beam.io.ReadFromText("./data/scores.csv",skip_header_lines=1)

        scores = pcoll_scores | "create schema scores" >> beam.Map(lambda row: to_scores(row)).with_output_types(Scores)
        accounts = pcoll_accounts | "create schema accounts" >> beam.Map(lambda row: to_account(row)).with_output_types(Account)
        
        accounts_si = pvalue.AsList(accounts)
        accounts_score = scores | beam.Map(broadcast_join,broad_table=accounts_si)
        accounts_avg_score = accounts_score | beam.GroupBy("account_name").aggregate_field(field="score",combine_fn=MeanCombineFn(),dest="avg_score")
        #accounts_filtered = accounts | beam.Select("name","age","email")
        #accounts_group = accounts | beam.GroupBy("gender").aggregate_field(field="age",combine_fn=max,dest="max_age")
        accounts_avg_score | "Print pcoll" >> beam.Map(print)
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
