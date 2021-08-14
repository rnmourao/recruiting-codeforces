from decimal import Decimal
import hashlib
import json
import random
from re import sub
import time

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
import pandas as pd
import requests

from tqdm import tqdm


PRIMARY_KEY = "handle"


def call_api(method, data, AK=None, SK=None):
    url = f"https://codeforces.com/api/{method}"

    if AK is not None:
        data["apiKey"] = AK
        data["time"] = int(time.time())

        rand = f"{random.randrange(0, 999999, 6) : 02d}"
        sorted_params = "&".join([f"{k}={data[k]}" for k in sorted(data)])
        code = f"{rand}/{method}?" + sorted_params + "#" + SK
        print(code)
        hash = hashlib.sha512(code.encode("utf-8")).hexdigest()
        data["apiSig"] = f"{rand}{hash}"
        print(data["apiSig"])

    response = requests.get(url, params=data)

    res = []
    try:
        result = response.json()
        if result["status"] == "FAILED":
            raise Exception(method, result["comment"])
        res = result["result"]
    except json.decoder.JSONDecodeError as e:
        print(method, data, e)
    return res


def get_updates():
    " Recovers codeforces data using their API."

    data = {"activeOnly": "true"}
    rated_list = call_api("user.ratedList", data)

    reachable = []
    for user in tqdm(rated_list):
        email = user.get("email", None)
        handle = user.get("handle", None)
        
        # filter reachable programmers
        if email and handle:
            data = {"handle": handle}
            # try:
            submissions = call_api("user.status", data)
            # except Exception as e:
                # continue
            languages = set()
            for submission in submissions:
                if submission["verdict"] == "OK":
                    language = submission["programmingLanguage"]
                    if "C++" in language:
                        languages.add("C++")
                    elif "Python" in language or "PyPy" in language:
                        languages.add("Python")
                    elif "Java" in language:
                        languages.add("Java")
                    else:
                        languages.add(language)
            user["languages"] = languages
            reachable.append(user)
        # for tests    
        if len(reachable) > 10:
            break

    df = pd.DataFrame(reachable)
    df = df[["handle", "firstName", "lastName", "email", "country", "maxRank", 
             "maxRating", "contribution", "languages"]]
    df = df.set_index(PRIMARY_KEY)
    return df


def find_differences(current, updates):
    changes = pd.DataFrame()
    report = dict()
    
    fields = ["email", "country", "maxRank", "maxRating", 
              "contribution", "languages"]
    upd = updates[fields]
    
    missing_fields = set(fields) - set(current.columns)
    for mf in missing_fields:
        current[mf] = None
    cur = current[fields]

    merged = cur.merge(upd, how="outer", left_index=True, right_index=True,
                       suffixes=("_cur", "_upd"), indicator=True)
    
    new_competitors = merged.loc[merged["_merge"] == "right_only"].index.tolist()
    
    inserts = updates.loc[new_competitors]
    for i, row in inserts.iterrows():
        report[i] = ["new_user"]
        changes = changes.append(row.to_dict(), ignore_index=True)

    for i, r in merged.loc[merged["_merge"] == "both"].iterrows():
        row = r.to_dict()
        new_row = current.loc[i].to_dict()
        ls = []
        for field in fields:
            new_row[field] = row[field + "_upd"]
            if row[field + "_cur"] != row[field + "_upd"]:
                ls.append(field)
        report[i] = ls.copy()
        changes = changes.append(new_row, ignore_index=True)
        
    return changes, report


def persist_changes(table, changes):
    with table.batch_writer() as batch:
        for i, row in changes.reset_index().iterrows():
            item = json.loads(row.to_json(), parse_float=Decimal)
            batch.put_item(item)


def build_email(report):
    message = ""

    #

    return message


def send_email(message):
    pass


if __name__ == "__main__":
    # configuration
    # my_config = Config(region_name='us-east-1')
    dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000") #, config=my_config)

    # database    
    table = dynamodb.Table('codeforces')

    # recover all database data
    current = pd.DataFrame(table.scan()["Items"]).set_index(PRIMARY_KEY)

    # get updates from codeforces.com
    updates = get_updates()
    updates.to_csv("data/updates.csv")

    # merge with indicator to find differences
    changes, report = find_differences(current, updates)

    # persist changes
    persist_changes(table, changes)

    # build email
    message = build_email(report)
    
    # send email
    send_email(message)