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
        report[i] = {"new_user": True}
        changes = changes.append(row.to_dict(), ignore_index=True)

    for i, r in merged.loc[merged["_merge"] == "both"].iterrows():
        row = r.to_dict()
        new_row = current.loc[i].to_dict()
        d = dict()
        for field in fields:
            old_value = row[field + "_cur"]
            new_value = row[field + "_upd"]
            new_row[field] = new_value
            if old_value != new_value:
                d[field] = (old_value, new_value)
        report[i] = d.copy()
        changes = changes.append(new_row, ignore_index=True)
        
    return changes, report


def persist_changes(table, changes):
    with table.batch_writer() as batch:
        for i, row in changes.reset_index().iterrows():
            item = json.loads(row.to_json(), parse_float=Decimal)
            batch.put_item(item)


def build_email(changes, report):
    should_send = False
    message = "<head><body>"

    new_users = []
    updated_users = []
    for key in report.keys():
        if report[key].has_key("new_user"):
            new_users.append(key)
            continue
        updated_users.append(key)
            
    if new_users:
        message += "<h2>New Users</h2>"
        message += changes.loc[new_users].to_html()
        should_send = True

    uudf = changes.loc[updated_users].copy()
    uudf["Remarks"] = ""
    important = []
    for i, row in uudf.iterrows():
        rmks = []
        if report[i].has_key("email"):
            rmks.append("*")
        if report[i].has_key("languages"):
            rmks.append("**")
        if report[i].has_key("maxRank"):
            rmks.append("***")
        if rmks:
            important.append(uudf.loc[i].reset_index().to_dict())
        row["Remarks"] = " ".join(rmks)

    if important:
        message += "<h2>Updates</h2>"
        message += pd.DataFrame(important).to_html()
        message += """<p>* The email address has changed.</p>
                    <p>** The languages list has changed.</p>
                    <p>*** The ranking has changed.</p>
        """
        should_send = True

    message += "</body></head>"

    if should_send:
        return message
    else:
        return ""


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

    # build and send email
    message = build_email(changes, report)
    if message:
        send_email(message)