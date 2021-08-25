#!/bin/python

import hashlib
from io import BytesIO
import json
import os
import random
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
                    elif language:
                        languages.add(language)
            user["languages"] = languages
            reachable.append(user)

    df = pd.DataFrame(reachable)
    df = df[["handle", "firstName", "lastName", "email", "country", "maxRank", 
             "maxRating", "contribution", "languages"]]
    df = df.loc[~pd.isna(df["handle"])]
    df = df.set_index(PRIMARY_KEY)
    return df


def find_differences(current, updates):
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
    
    ls = []
    for i, row in merged.iterrows():
        new_row = {field: None for field in fields}
        new_row["handle"] = i
        diff = dict()
        for field in fields:
            if row["_merge"] == "left_only":
                new_row[field] = row[field + "_cur"]
            else:
                new_row[field] = row[field + "_upd"]
                if row["_merge"] == "both":
                    old_value = row[field + "_cur"]
                    new_value = row[field + "_upd"]
                    if old_value != new_value:
                        diff[field] = (old_value, new_value)
        ls.append(new_row.copy())
        if row["_merge"] == "right_only":
            report[i] = {"new_user": True}
        if diff:
            report[i] = diff.copy()
    new_df = pd.DataFrame(ls)
    new_df = new_df.set_index("handle")
    new_df["maxRating"] = new_df["maxRating"].astype("int64")
    new_df["contribution"] = new_df["contribution"].astype("int64")
    return new_df, report


def build_email(df, report):
    should_send = False
    message = "<html><head><body>"
    df["languages"] = df.apply(lambda r: ", ".join(r["languages"]), axis=1)

    new_users = []
    updated_users = []
    for key in report.keys():
        if "new_user" in report[key]:
            new_users.append(key)
            continue
        updated_users.append(key)
            
    if new_users:
        message += "<h2>New Users</h2>"
        nudf = df.loc[new_users].copy()
        nudf = nudf.loc[nudf["maxRating"] > 2000]
        nudf = nudf.sort_values(by="maxRating", ascending=False)
        nudf.index.name = None
        message += nudf.to_html()
        should_send = True

    uudf = df.loc[updated_users].copy()
    uudf["remarks"] = ""
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
        idf = pd.DataFrame(important) \
                     .set_index("handle") \
                     .sort_values(by="maxRating", ascending=False)
        idf.index.name = None
        message += idf.to_html()
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
    AWS_REGION = "us-east-1"
    CHARSET = "UTF-8"
    SUBJECT = "Codeforces Weekly Update"
    BODY_HTML = message            
    client = boto3.client('ses', region_name=AWS_REGION)

    response = client.list_identities(IdentityType='EmailAddress')
    emails = response["Identities"]
    SENDER = [e for e in emails if "contato" in e][0]
    emails.remove(SENDER)
    RECIPIENT = emails

    # The email body for recipients with non-HTML email clients.
    BODY_TEXT = ("")
                
    try:
        #Provide the contents of the email.
        response = client.send_email(
            Destination={
                'ToAddresses': RECIPIENT,
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


def load_data():
    bucket = os.environ["S3_REPOSITORY"]
    s3 = boto3.client('s3', region_name="us-east-1")
    buffer = BytesIO()
    s3.download_fileobj(bucket, "codeforces.parquet", buffer)
    df = pd.read_parquet(buffer)
    df["languages"] = df.apply(lambda r: set(r["languages"]), axis=1)
    return df


def save_data(df):
    df["languages"] = df.apply(lambda r: list(r["languages"]), axis=1)
    bucket = os.environ["S3_REPOSITORY"]
    s3 = boto3.client('s3', region_name="us-east-1")
    buffer = BytesIO()
    df.to_parquet(path=buffer)
    buffer.seek(0)
    s3.upload_fileobj(Fileobj=buffer, Bucket=bucket, Key="codeforces.parquet")


if __name__ == "__main__":
    # recover all database data
    current = load_data()

    # get updates from codeforces.com
    updates = get_updates()
    
    # merge with indicator to find differences
    new_df, report = find_differences(current, updates)

    # persist changes
    save_data(new_df)

    # build and send email
    message = build_email(new_df, report)
    if message:
        send_email(message)