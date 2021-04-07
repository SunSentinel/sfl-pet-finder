import io
import csv
import json
from datetime import datetime

import boto3
import botocore
from esridump.dumper import EsriDumper


def main(event, context):
    d = EsriDumper("https://services.arcgis.com/JMAJrTsHNLrSsWf5/arcgis/rest/services/Animal_Care_Intake_and_Outcome_Data/FeatureServer/0")
    pet_list = []
    for feature in d:
        if feature["properties"]["OutcomeWeightDate"] > 0:
            feature["properties"]["OutcomeWeightDate"] = to_datetime(feature["properties"]["OutcomeWeightDate"])
            feature["properties"]["Extra11"] = to_datetime(feature["properties"]["Extra11"])
            feature["properties"]["LatestUpdate"] = to_datetime(feature["properties"]["LatestUpdate"])
            feature["properties"]["IntakeWeightDate"] = to_datetime(feature["properties"]["IntakeWeightDate"])
            feature["properties"]["IntakeDate"] = to_datetime(feature["properties"]["IntakeDate"])
            feature["properties"]["OutcomeDate"] = to_datetime(feature["properties"]["OutcomeDate"])
            feature["properties"]["WeightDate"] = to_datetime(feature["properties"]["WeightDate"])

            pet_list.append(feature["properties"])
    
    save_S3_sheet(pet_list, "broward_pets.csv")
    

def save_S3_sheet(input_data, filename):

    AWS_BUCKET_NAME = 'content.sun-sentinel.com'
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(AWS_BUCKET_NAME)
    path = "data/pet-finder/" + filename
    
    with io.StringIO() as csvfile:
        headers = set(list(input_data[0].keys()) + list(input_data[-1].keys()))
        csv_writer = csv.DictWriter(csvfile, fieldnames=headers)
        csv_writer.writeheader()
        for row in input_data:
            csv_writer.writerow(row)

        data = csvfile.getvalue()

        bucket.put_object(Key=path, Body=data)




def to_datetime(dt):
    return datetime.fromtimestamp(dt/1000)