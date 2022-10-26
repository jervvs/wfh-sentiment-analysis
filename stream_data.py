import requests
import json
from config import BEARER_TOKEN
import base64
import boto3

kinesis = boto3.client('kinesis')

def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """
    r.headers["Authorization"] = f"Bearer {BEARER_TOKEN}"
    r.headers["User-Agent"] = "v2FilteredStreamPython"
    return r

def get_rules():
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream/rules", auth=bearer_oauth
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot get rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print(json.dumps(response.json()))
    return response.json()


def delete_all_rules(rules):
    if rules is None or "data" not in rules:
        return None

    ids = list(map(lambda rule: rule["id"], rules["data"]))
    payload = {"delete": {"ids": ids}}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot delete rules (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    print(json.dumps(response.json()))

def reset_rules():
    rules = get_rules()
    delete_all_rules(rules)

def set_rules(rules):
    payload = {"add": rules}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload,
    )
    if response.status_code != 201:
        raise Exception(
            "Cannot add rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print(json.dumps(response.json()))

def get_stream():
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream?tweet.fields=created_at", auth=bearer_oauth, stream=True,
    )
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(
            "Cannot get stream (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    for response_line in response.iter_lines():
        if response_line:
            json_response = json.loads(response_line)
            text = json_response['data']['text']
            encoded_text = base64.b64encode(text.encode('utf-8')).decode('utf-8')
            # 1. convert string to a bytes-like object
            # 2. Base64 encode the bytes-like object
            # 3. Get string representation of the Base64 conversion by decoding
            record = {
                "id": json_response['data']['id'],
                "timestamp": json_response['data']['created_at'],
                "tweet": encoded_text
            }
            json_record = json.dumps(record, indent=4) + "|"
            try:
                kinesis.put_record(
                    StreamName='twitterStream',
                    Data=json_record,
                    PartitionKey='key',
                )
                print(text)
                print(json_record)

            except Exception as e:
                print(e.message, e.args)

def main():
    # 1. Reset Any Past Twitter Rules
    reset_rules()

    # 2. Set New Rules
    # Can have up to 5 rules
    rules = [
        {"value": "wfh -is:retweet lang:en", "tag": "work from home short form"},
        {"value": "work from home -is:retweet lang:en", "tag": "work from home"},
    ]
    set_rules(rules)

    # 3. Stream Data From Twitter and Put into Kinesis Data Streams
    get_stream()


if __name__ == "__main__":
    main()