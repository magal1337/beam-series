#!/bin/bash

python3 main.py \
--input_flights=gs://beam-data-poc/input/full/*.csv \
--input_airports=gs://beam-data-poc/input/airports.parquet \
--output=gs://beam-data-poc/output/flights_agg.csv \
--runner=DataflowRunner \
--project=poc-beam-370923 \
--region=us-east1 \
--temp_location=gs://beam-data-poc/tmp/ \
--machine_type=n2-standard-2
