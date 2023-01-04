from apache_beam.dataframe.transforms import DataframeTransform
import apache_beam.coders as coders
from apache_beam.dataframe.io import read_csv, to_csv
from apache_beam.dataframe.convert import to_pcollection, to_dataframe
from apache_beam.transforms.sql import SqlTransform
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import typing
import argparse

class Flight(typing.NamedTuple):
    flight_date: str
    flight_number_reporting_airline: int
    origin_city_name: str
    origin: str
    dest_city_name: str
    dest: str
    elapse_time: typing.Optional[float]
    distance: typing.Optional[int]

class Airport(typing.NamedTuple):
    iata: str
    name: str
    city: str
    country: str
    altitude: int

coders.registry.register_coder(Flight, coders.RowCoder)
coders.registry.register_coder(Airport, coders.RowCoder)

def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_flights",required=True)
    parser.add_argument("--input_airports",required=True)
    parser.add_argument("--output",required=True)
    know_args, pipeline_args = parser.parse_known_args()

    pipeline_options = PipelineOptions(pipeline_args,save_main_session=True)
    with beam.Pipeline(options=pipeline_options) as p:

        flights = p | "READ FLIGHTS CSV" >> read_csv(know_args.input_flights,sep=",")
        flights = flights[(flights["Cancelled"] == 0.0) & (flights["Distance"] > 1000)]
        flights_pcoll = to_pcollection(flights) |"CONVERT TO FLIGHT CLASS" >> beam.Map(lambda x: Flight(
                   flight_date=x.FlightDate,
                   flight_number_reporting_airline=x.Flight_Number_Reporting_Airline,
                   origin_city_name=x.OriginCityName,
                   origin=x.Origin,
                   dest_city_name=x.DestCityName,
                   dest=x.Dest,
                   elapse_time=x.ActualElapsedTime,
                   distance=x.Distance
        )).with_output_types(Flight)

        airports_pcoll = p | "READ AIRPORTS PARQUET" >> beam.io.ReadFromParquet(
                know_args.input_airports
        )
        
        airports_with_schema = airports_pcoll | "CONVERT TO AIRPORT CLASS" >> beam.Map(
                lambda x: Airport(
                    iata=x["iata"],
                    name=x["name"],
                    city=x["city"],
                    country=x["country"],
                    altitude=x["altitude"]
                )                
        ).with_output_types(Airport)

 
        flights_with_altitude = {"flights": flights_pcoll,"airports": airports_with_schema} | \
            "JOIN FLIGHTS WITH AIRPORTS PCOLL" >> SqlTransform(
                """
                WITH joined_with_origin_altitude AS ( 
                    SELECT 
                        flights.*, 
                        airports_ori.altitude AS origin_altitude
                    FROM 
                        flights
                    LEFT JOIN 
                        airports AS airports_ori
                    ON 
                        flights.origin = airports_ori.iata
                ),

                joined_with_dest_altitude AS ( 
                SELECT 
                    joined.*,
                    airports_dest.altitude AS dest_altitude 
                FROM 
                    joined_with_origin_altitude AS joined 
                LEFT JOIN
                    airports AS airports_dest
                ON 
                    joined.dest = airports_dest.iata
                )

                SELECT 
                    origin,
                    ABS(dest_altitude - origin_altitude) AS abs_altitude_diff
                FROM 
                    joined_with_dest_altitude
                WHERE
                    origin_altitude IS NOT NULL AND dest_altitude IS NOT NULL
                LIMIT 1000
                """
        )
        flights_agg = flights_with_altitude | "MEAN ALTITUDE DIFF FLIGHTS BY ORIGIN" >> DataframeTransform(
                lambda df: df.groupby(["origin"]).mean()
        )

        flights_agg_df = to_dataframe(flights_agg)
        flights_agg | beam.Map(print)
        to_csv(flights_agg_df,path=know_args.output)




if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()


