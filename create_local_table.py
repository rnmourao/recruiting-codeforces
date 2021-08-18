from decimal import Decimal
import json
import pandas as pd
import boto3
from botocore.exceptions import ClientError


def create_local_table(dynamodb):
    table = dynamodb.create_table(
        TableName='codeforces',
        KeySchema=[
            {
                'AttributeName': 'handle',
                'KeyType': 'HASH'  # Partition key
            },
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'handle',
                'AttributeType': 'S'
            },
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
    return table


def populate_local_table(df, table):
    with table.batch_writer() as batch:
        for _, row in df.iterrows():
            item = json.loads(row.to_json(), parse_float=Decimal)
            if item["handle"] is None:
                continue
            batch.put_item(item)


if __name__ == '__main__':
    dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000")

    table = create_local_table(dynamodb)
    print("Table status:", table.table_status)

    df = pd.read_csv("data/codeforces.csv", converters={"languages": eval})
    populate_local_table(df, table)
