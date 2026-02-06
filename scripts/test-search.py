#!/usr/bin/env python3
"""Test script for the search indexing pipeline.

Usage:
    python scripts/test-search.py list-emails <account-id>
    python scripts/test-search.py index <account-id> <email-id>
    python scripts/test-search.py search <account-id> "search terms"

Requires: pip install boto3
Expects: AWS_PROFILE=ses-mail (or configured in ~/.aws/config)
"""

import json
import sys

import boto3

REGION = "ap-southeast-2"
DYNAMODB_TABLE = "jmap-service-email-data-test"
SQS_QUEUE_NAME = "jmap-service-email-test-search-index"
S3V_BUCKET = "jmap-service-email-test-search-vectors"
BEDROCK_MODEL = "amazon.titan-embed-text-v2:0"
SSM_API_URL_PARAM = "/jmap-service-core/test/api-gateway-invoke-url"


def get_session():
    return boto3.Session(region_name=REGION)


def cmd_list_emails(account_id):
    session = get_session()
    dynamodb = session.client("dynamodb")

    pk = f"ACCOUNT#{account_id}"
    sk_prefix = "EMAIL#"

    items = []
    last_key = None
    while True:
        kwargs = {
            "TableName": DYNAMODB_TABLE,
            "KeyConditionExpression": "pk = :pk AND begins_with(sk, :skPrefix)",
            "ExpressionAttributeValues": {
                ":pk": {"S": pk},
                ":skPrefix": {"S": sk_prefix},
            },
            "ProjectionExpression": "emailId, subject, #f, receivedAt",
            "ExpressionAttributeNames": {"#f": "from"},
        }
        if last_key:
            kwargs["ExclusiveStartKey"] = last_key

        resp = dynamodb.query(**kwargs)
        items.extend(resp.get("Items", []))
        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break

    if not items:
        print(f"No emails found for account {account_id}")
        return

    print(f"Found {len(items)} email(s) for account {account_id}:\n")
    for item in items:
        email_id = item.get("emailId", {}).get("S", "?")
        subject = item.get("subject", {}).get("S", "(no subject)")
        received = item.get("receivedAt", {}).get("S", "?")

        from_field = item.get("from", {}).get("L", [])
        from_str = ""
        if from_field:
            first = from_field[0].get("M", {})
            name = first.get("name", {}).get("S", "")
            email = first.get("email", {}).get("S", "")
            from_str = f"{name} <{email}>" if name else email

        print(f"  {email_id}")
        print(f"    Subject:  {subject}")
        print(f"    From:     {from_str}")
        print(f"    Received: {received}")
        print()


def cmd_index(account_id, email_id):
    session = get_session()
    sqs = session.client("sqs")
    ssm = session.client("ssm")

    # Get API URL from SSM
    resp = ssm.get_parameter(Name=SSM_API_URL_PARAM)
    api_url = resp["Parameter"]["Value"]
    print(f"API URL: {api_url}")

    # Get SQS queue URL
    resp = sqs.get_queue_url(QueueName=SQS_QUEUE_NAME)
    queue_url = resp["QueueUrl"]

    # Send message
    message = {
        "accountId": account_id,
        "emailId": email_id,
        "action": "index",
        "apiUrl": api_url,
    }
    resp = sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(message))
    msg_id = resp["MessageId"]
    print(f"Sent SQS message: {msg_id}")
    print(f"  Account: {account_id}")
    print(f"  Email:   {email_id}")
    print(f"\nCheck CloudWatch logs for /aws/lambda/jmap-service-email-test-email-index")


def cmd_search(account_id, query_text):
    session = get_session()

    # Get query embedding from Bedrock
    bedrock = session.client("bedrock-runtime")
    resp = bedrock.invoke_model(
        modelId=BEDROCK_MODEL,
        contentType="application/json",
        accept="application/json",
        body=json.dumps({"inputText": query_text}),
    )
    body = json.loads(resp["body"].read())
    embedding = body["embedding"]
    print(f"Generated embedding ({len(embedding)} dimensions) for: {query_text!r}\n")

    # Query S3 Vectors
    s3v = session.client("s3vectors")
    index_name = f"acct-{account_id}"
    try:
        resp = s3v.query_vectors(
            vectorBucketName=S3V_BUCKET,
            indexName=index_name,
            queryVector={"float32": embedding},
            topK=10,
            returnMetadata=True,
        )
    except s3v.exceptions.ClientError as e:
        if "NoSuchVectorIndex" in str(e) or "NoSuchKey" in str(e):
            print(f"Vector index '{index_name}' not found. Has any email been indexed for this account?")
            return
        raise

    vectors = resp.get("vectors", [])
    if not vectors:
        print("No results found.")
        return

    print(f"Found {len(vectors)} result(s):\n")
    for v in vectors:
        key = v.get("key", "?")
        score = v.get("distance", 0)
        metadata = v.get("metadata", {})
        email_id = metadata.get("emailId", "?")
        subject = metadata.get("subject", "?")
        from_val = metadata.get("from", "?")
        chunk_idx = metadata.get("chunkIndex", "?")

        print(f"  Score: {score:.4f}  Email: {email_id}")
        print(f"    Subject: {subject}")
        print(f"    From:    {from_val}")
        print(f"    Chunk:   {chunk_idx}")
        print()


def usage():
    print(__doc__.strip())
    sys.exit(1)


def main():
    if len(sys.argv) < 2:
        usage()

    command = sys.argv[1]

    if command == "list-emails":
        if len(sys.argv) != 3:
            print("Usage: test-search.py list-emails <account-id>")
            sys.exit(1)
        cmd_list_emails(sys.argv[2])

    elif command == "index":
        if len(sys.argv) != 4:
            print("Usage: test-search.py index <account-id> <email-id>")
            sys.exit(1)
        cmd_index(sys.argv[2], sys.argv[3])

    elif command == "search":
        if len(sys.argv) != 4:
            print("Usage: test-search.py search <account-id> \"search terms\"")
            sys.exit(1)
        cmd_search(sys.argv[2], sys.argv[3])

    else:
        print(f"Unknown command: {command}")
        usage()


if __name__ == "__main__":
    main()
