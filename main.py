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
        new_row = row.to_dict()
        new_row["handle"] = i
        changes = changes.append(new_row, ignore_index=True)

    for i, r in merged.loc[merged["_merge"] == "both"].iterrows():
        row = r.to_dict()
        new_row = current.loc[i].to_dict()
        new_row["handle"] = i
        d = dict()
        for field in fields:
            old_value = row[field + "_cur"]
            new_value = row[field + "_upd"]
            new_row[field] = new_value
            if old_value != new_value:
                d[field] = (old_value, new_value)
        if d:
            report[i] = d.copy()
        changes = changes.append(new_row, ignore_index=True)
    changes = changes.set_index(PRIMARY_KEY)
    return changes, report


def persist_changes(table, changes):
    with table.batch_writer() as batch:
        for i, row in changes.reset_index().iterrows():
            item = json.loads(row.to_json(), parse_float=Decimal)
            batch.put_item(item)


def build_email(changes, report):
    should_send = False
    message = "<html><head><body>"
    try:
        changes["languages"] = changes.apply(lambda r: ", ".join(eval(r["languages"])), axis=1)
    except TypeError:
        changes["languages"] = changes.apply(lambda r: ", ".join(r["languages"]), axis=1)

    new_users = []
    updated_users = []
    for key in report.keys():
        if "new_user" in report[key]:
            new_users.append(key)
            continue
        updated_users.append(key)
            
    if new_users:
        message += "<h2>New Users</h2>"
        nudf = changes.loc[new_users]
        nudf = nudf.loc[nudf["maxRating"] > 2000]
        nudf = nudf.sort_values(by="maxRating", ascending=False)
        message += nudf.to_html(index=False)
        should_send = True

    uudf = changes.loc[updated_users].copy()
    uudf["Remarks"] = ""
    important = []
    for i, row in uudf.iterrows():

        rmks = []
        if "email" in report[i]:
            rmks.append("*")
        if "languages" in report[i]:
            rmks.append("**")
        if "maxRank" in report[i]:
            rmks.append("***")
        if rmks:
            if row["maxRating"] > 2000:
                new_row = uudf.loc[i].to_dict()
                new_row["handle"] = i
                new_row["remarks"] = " ".join(rmks)
                important.append(new_row)

    if important:
        if new_users:
            message += "<br>"
        message += "<h2>Updates</h2>"
        message += pd.DataFrame(important) \
                     .sort_values(by="maxRating", ascending=False) \
                     .to_html(index=False)
        message += """<p>* The email address has changed.</p>
                    <p>** The languages list has changed.</p>
                    <p>*** The ranking has changed.</p>
        """
        should_send = True

    message += "</body></head></html>"

    if should_send:
        return message
    else:
        return ""


def send_email(message):
    SENDER = ""
    RECIPIENT = ""
    AWS_REGION = "us-east-1"
    CHARSET = "UTF-8"
    SUBJECT = "Codeforces Weekly Update"
    BODY_HTML = message            
    client = boto3.client('ses', region_name=AWS_REGION)

    # The email body for recipients with non-HTML email clients.
    BODY_TEXT = ("")
                
    try:
        #Provide the contents of the email.
        response = client.send_email(
            Destination={
                'ToAddresses': [
                    RECIPIENT,
                ],
            },
            Message={
                'Body': {
                    'Html': {
                        'Charset': CHARSET,
                        'Data': BODY_HTML,
                    },
                    'Text': {
                        'Charset': CHARSET,
                        'Data': BODY_TEXT,
                    },
                },
                'Subject': {
                    'Charset': CHARSET,
                    'Data': SUBJECT,
                },
            },
            Source=SENDER,
        )
    # Display an error if something goes wrong.	
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID:"),
        print(response['MessageId'])


if __name__ == "__main__":
    # configuration
    # my_config = Config(region_name='us-east-1')
    dynamodb = boto3.resource('dynamodb', endpoint_url="http://localhost:8000") #, config=my_config)

    # database    
    table = dynamodb.Table('codeforces')

    # recover all database data
    current = pd.DataFrame(table.scan()["Items"])
    current = current.set_index(PRIMARY_KEY)
    try:
        current["languages"] = current.apply(lambda r: eval(r["languages"]), axis=1)
    except TypeError:
        current["languages"] = current.apply(lambda r: set(r["languages"]), axis=1)

    # get updates from codeforces.com
    try:
        updates = pd.read_csv("data/updates.csv")
        updates["languages"] = updates.apply(lambda r: eval(r["languages"]), axis=1)
    except FileNotFoundError:
        updates = get_updates()
        updates = updates.loc[~pd.isna(updates["handle"])]
        updates.to_csv("data/updates.csv")
    updates = updates.set_index(PRIMARY_KEY)

    # merge with indicator to find differences
    changes, report = find_differences(current, updates)

    # # persist changes
    persist_changes(table, changes)

    # # # build and send email
    message = build_email(changes, report)
    # with open("data/message.html", "w") as w:
        # w.write(message)
    if message:
        print("have a message!")
        send_email(message)