#!/usr/bin/env python3
"""Test script for the search indexing pipeline.

Usage:
    python scripts/test-search.py list-emails <account-id>
    python scripts/test-search.py show-email <account-id> <email-id>
    python scripts/test-search.py index <account-id> <email-id>
    python scripts/test-search.py search <account-id> "search terms" [--type body|subject] [--filter '{}']
    python scripts/test-search.py list-tokens <account-id> [field] [prefix]
    python scripts/test-search.py get-vectors <account-id> <email-id>
    python scripts/test-search.py query <account-id> '<filter-json>'
    python scripts/test-search.py run-tests <account-id> <email-id>

Requires: pip install boto3
Expects: AWS_PROFILE=ses-mail (or configured in ~/.aws/config)
"""

import argparse
import json
import sys

import boto3

REGION = "ap-southeast-2"
DYNAMODB_TABLE = "jmap-service-email-data-test"
SQS_QUEUE_NAME = "jmap-service-email-test-search-index"
S3V_BUCKET = "jmap-service-email-test-search-vectors"
BEDROCK_MODEL = "amazon.titan-embed-text-v2:0"
SSM_API_URL_PARAM = "/jmap-service-core/test/api-gateway-invoke-url"
EMAIL_QUERY_LAMBDA = "jmap-service-email-test-email-query"


def get_session():
    return boto3.Session(region_name=REGION)


# --- Helper functions ---


def format_addresses(dynamo_list):
    """Format a DynamoDB L-of-M address list as 'Name <email>, ...'."""
    parts = []
    for item in dynamo_list:
        m = item.get("M", {})
        name = m.get("name", {}).get("S", "")
        email = m.get("email", {}).get("S", "")
        if name:
            parts.append(f"{name} <{email}>")
        else:
            parts.append(email)
    return ", ".join(parts) if parts else "(none)"


def format_map_keys(dynamo_map):
    """Comma-join keys of a DynamoDB M attribute (for mailboxIds/keywords)."""
    if not dynamo_map:
        return "(none)"
    keys = list(dynamo_map.keys())
    return ", ".join(keys) if keys else "(none)"


def parse_token_sk(sk):
    """Split TOK#FIELD#token#RCVD#ts#emailId into components.

    Uses #RCVD# as the safe delimiter since tokens may contain # characters.
    """
    # Remove TOK# prefix
    rest = sk[4:]  # after "TOK#"
    # Split on #RCVD# to separate field+token from ts+emailId
    parts = rest.split("#RCVD#", 1)
    if len(parts) != 2:
        return {"field": "?", "token": "?", "receivedAt": "?", "emailId": "?"}
    field_token = parts[0]
    ts_email = parts[1]
    # field_token is FIELD#token
    field_sep = field_token.index("#")
    field = field_token[:field_sep]
    token = field_token[field_sep + 1 :]
    # ts_email is timestamp#emailId - timestamp is RFC3339 with known format
    # Find the last # to split emailId (emailId won't contain #)
    last_hash = ts_email.rindex("#")
    received_at = ts_email[:last_hash]
    email_id = ts_email[last_hash + 1 :]
    return {
        "field": field,
        "token": token,
        "receivedAt": received_at,
        "emailId": email_id,
    }


def get_email_item(dynamodb, account_id, email_id):
    """Fetch a full email item from DynamoDB."""
    resp = dynamodb.get_item(
        TableName=DYNAMODB_TABLE,
        Key={
            "pk": {"S": f"ACCOUNT#{account_id}"},
            "sk": {"S": f"EMAIL#{email_id}"},
        },
    )
    return resp.get("Item")


# --- Commands ---


def cmd_list_emails(args):
    account_id = args.account_id
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
        from_str = format_addresses(item.get("from", {}).get("L", []))

        print(f"  {email_id}")
        print(f"    Subject:  {subject}")
        print(f"    From:     {from_str}")
        print(f"    Received: {received}")
        print()


def cmd_show_email(args):
    account_id = args.account_id
    email_id = args.email_id
    session = get_session()
    dynamodb = session.client("dynamodb")

    item = get_email_item(dynamodb, account_id, email_id)
    if not item:
        print(f"Email {email_id} not found in account {account_id}")
        sys.exit(1)

    deleted_at = item.get("deletedAt", {}).get("S", "")
    if deleted_at:
        print(f"*** WARNING: This email was deleted at {deleted_at} ***\n")

    subject = item.get("subject", {}).get("S", "(no subject)")
    from_str = format_addresses(item.get("from", {}).get("L", []))
    to_str = format_addresses(item.get("to", {}).get("L", []))
    cc_str = format_addresses(item.get("cc", {}).get("L", []))
    received = item.get("receivedAt", {}).get("S", "?")
    size = item.get("size", {}).get("N", "?")
    has_attach = item.get("hasAttachment", {}).get("BOOL", False)
    mailbox_ids = format_map_keys(item.get("mailboxIds", {}).get("M", {}))
    keywords = format_map_keys(item.get("keywords", {}).get("M", {}))
    search_chunks = item.get("searchChunks", {}).get("N", "0")
    preview = item.get("preview", {}).get("S", "(none)")

    text_body = item.get("textBody", {}).get("L", [])
    text_body_ids = ", ".join(p.get("S", "?") for p in text_body) if text_body else "(none)"

    print(f"Email: {email_id}")
    print(f"  Subject:       {subject}")
    print(f"  From:          {from_str}")
    print(f"  To:            {to_str}")
    print(f"  CC:            {cc_str}")
    print(f"  Received:      {received}")
    print(f"  Size:          {size} bytes")
    print(f"  HasAttachment: {has_attach}")
    print(f"  MailboxIds:    {mailbox_ids}")
    print(f"  Keywords:      {keywords}")
    print(f"  SearchChunks:  {search_chunks}")
    print(f"  TextBody:      {text_body_ids}")
    print(f"  Preview:       {preview}")


def cmd_index(args):
    account_id = args.account_id
    email_id = args.email_id
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


def cmd_search(args, return_results=False):
    account_id = args.account_id
    query_text = args.query
    vector_type = getattr(args, "type", None)
    extra_filter = getattr(args, "filter", None)

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
    if not return_results:
        print(f"Generated embedding ({len(embedding)} dimensions) for: {query_text!r}\n")

    # Build metadata filter
    metadata_filter = {}
    if vector_type:
        metadata_filter["type"] = {"$eq": vector_type}
    if extra_filter:
        user_filter = json.loads(extra_filter)
        metadata_filter.update(user_filter)

    # Query S3 Vectors
    s3v = session.client("s3vectors")
    index_name = f"acct-{account_id}"
    query_kwargs = {
        "vectorBucketName": S3V_BUCKET,
        "indexName": index_name,
        "queryVector": {"float32": embedding},
        "topK": 10,
        "returnMetadata": True,
    }
    if metadata_filter:
        query_kwargs["filter"] = metadata_filter

    try:
        resp = s3v.query_vectors(**query_kwargs)
    except s3v.exceptions.ClientError as e:
        if "NoSuchVectorIndex" in str(e) or "NoSuchKey" in str(e):
            msg = f"Vector index '{index_name}' not found. Has any email been indexed for this account?"
            if return_results:
                print(f"  {msg}")
                return []
            print(msg)
            return
        raise

    vectors = resp.get("vectors", [])
    if return_results:
        return vectors

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
        vec_type = metadata.get("type", "(unset)")
        chunk_idx = metadata.get("chunkIndex", "?")

        print(f"  Score: {score:.4f}  Type: {vec_type}  Email: {email_id}")
        print(f"    Subject: {subject}")
        print(f"    From:    {from_val}")
        print(f"    Chunk:   {chunk_idx}")
        print()


def cmd_list_tokens(args):
    account_id = args.account_id
    field = args.field
    prefix = args.prefix

    session = get_session()
    dynamodb = session.client("dynamodb")

    pk = f"ACCOUNT#{account_id}"
    # Build SK prefix
    sk_prefix = "TOK#"
    if field:
        sk_prefix += field.upper() + "#"
        if prefix:
            sk_prefix += prefix.lower()

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
        }
        if last_key:
            kwargs["ExclusiveStartKey"] = last_key

        resp = dynamodb.query(**kwargs)
        items.extend(resp.get("Items", []))
        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break

    if not items:
        print(f"No tokens found (prefix: {sk_prefix})")
        return

    print(f"Found {len(items)} token(s):\n")
    # Table header
    print(f"  {'FIELD':<6} {'TOKEN':<30} {'RECEIVED':<26} {'EMAIL ID'}")
    print(f"  {'-----':<6} {'-----':<30} {'--------':<26} {'--------'}")
    for item in items:
        sk = item.get("sk", {}).get("S", "")
        parsed = parse_token_sk(sk)
        print(
            f"  {parsed['field']:<6} {parsed['token']:<30} "
            f"{parsed['receivedAt']:<26} {parsed['emailId']}"
        )


def cmd_get_vectors(args):
    account_id = args.account_id
    email_id = args.email_id

    session = get_session()
    dynamodb = session.client("dynamodb")
    s3v = session.client("s3vectors")

    # Get email to find searchChunks count
    item = get_email_item(dynamodb, account_id, email_id)
    if not item:
        print(f"Email {email_id} not found in account {account_id}")
        sys.exit(1)

    search_chunks = int(item.get("searchChunks", {}).get("N", "0"))
    if search_chunks == 0:
        print(f"Email {email_id} has searchChunks=0 (not indexed yet)")
        return

    # Build vector keys: {emailId}#0, #1, ..., {emailId}#subject
    keys = [f"{email_id}#{i}" for i in range(search_chunks)]
    keys.append(f"{email_id}#subject")

    index_name = f"acct-{account_id}"
    print(f"Fetching {len(keys)} vector(s) from index '{index_name}':\n")

    try:
        resp = s3v.get_vectors(
            vectorBucketName=S3V_BUCKET,
            indexName=index_name,
            keys=keys,
            returnData=False,
            returnMetadata=True,
        )
    except Exception as e:
        if "NoSuchVectorIndex" in str(e):
            print(f"Vector index '{index_name}' not found.")
            return
        raise

    vectors = resp.get("vectors", [])
    if not vectors:
        print("No vectors found (index may not exist yet).")
        return

    for v in vectors:
        key = v.get("key", "?")
        metadata = v.get("metadata", {})
        print(f"  Key: {key}")
        for mk, mv in sorted(metadata.items()):
            print(f"    {mk}: {mv}")
        print()


def cmd_query(args):
    account_id = args.account_id
    filter_json = args.filter_json

    try:
        parsed_filter = json.loads(filter_json)
    except json.JSONDecodeError as e:
        print(f"Invalid JSON filter: {e}")
        sys.exit(1)

    return invoke_query(account_id, parsed_filter)


def invoke_query(account_id, query_filter):
    """Invoke Email/query Lambda and return (ids, response_args) or None on error."""
    result = invoke_query_raw(account_id, query_filter)
    if result is None:
        return None
    name, resp_args = result
    if name == "error":
        print(f"JMAP error: type={resp_args.get('type')}, description={resp_args.get('description')}")
        return None
    ids = resp_args.get("ids", [])
    return ids, resp_args


def invoke_query_raw(account_id, query_filter):
    """Invoke Email/query Lambda and return (name, response_args) or None on Lambda error."""
    session = get_session()
    lambda_client = session.client("lambda")

    payload = {
        "requestId": "test-manual",
        "callIndex": 0,
        "accountId": account_id,
        "method": "Email/query",
        "args": {"filter": query_filter},
        "clientId": "c0",
    }

    resp = lambda_client.invoke(
        FunctionName=EMAIL_QUERY_LAMBDA,
        InvocationType="RequestResponse",
        Payload=json.dumps(payload),
    )

    # Check for Lambda-level errors
    if resp.get("FunctionError"):
        error_body = json.loads(resp["Payload"].read())
        print(f"Lambda error: {json.dumps(error_body, indent=2)}")
        return None

    result = json.loads(resp["Payload"].read())
    method_resp = result.get("methodResponse", {})
    name = method_resp.get("name", "")
    resp_args = method_resp.get("args", {})

    return name, resp_args


def cmd_query_print(args):
    """Query command that prints results."""
    result = cmd_query(args)
    if result is None:
        return

    ids, resp_args = result
    position = resp_args.get("position", 0)
    total = resp_args.get("total", "?")

    print(f"Results: {len(ids)} ids (position={position}, total={total})\n")
    for i, eid in enumerate(ids):
        print(f"  {i}: {eid}")


def cmd_run_tests(args):
    account_id = args.account_id
    email_id = args.email_id

    session = get_session()
    dynamodb = session.client("dynamodb")

    # --- Setup: fetch email data ---
    print(f"Fetching email {email_id} for test setup...\n")
    item = get_email_item(dynamodb, account_id, email_id)
    if not item:
        print(f"FATAL: Email {email_id} not found in account {account_id}")
        sys.exit(1)

    subject = item.get("subject", {}).get("S", "")
    if not subject:
        print("FATAL: Email has no subject")
        sys.exit(1)

    from_list = item.get("from", {}).get("L", [])
    from_email = ""
    if from_list:
        from_email = from_list[0].get("M", {}).get("email", {}).get("S", "")

    mailbox_map = item.get("mailboxIds", {}).get("M", {})
    first_mailbox = list(mailbox_map.keys())[0] if mailbox_map else ""

    keyword_map = item.get("keywords", {}).get("M", {})
    first_keyword = list(keyword_map.keys())[0] if keyword_map else ""

    search_chunks = int(item.get("searchChunks", {}).get("N", "0"))
    if search_chunks == 0:
        print("FATAL: searchChunks=0 — email has not been indexed yet.")
        print("Run: python scripts/test-search.py index", account_id, email_id)
        sys.exit(1)

    # Use first few words of subject as search query
    subject_words = " ".join(subject.split()[:4])

    print(f"  Subject:      {subject}")
    print(f"  Search query: {subject_words!r}")
    print(f"  From:         {from_email}")
    print(f"  Mailbox:      {first_mailbox}")
    print(f"  Keyword:      {first_keyword}")
    print(f"  SearchChunks: {search_chunks}")
    print()

    results = []

    # --- A. Vector search tests ---
    def run_search_test(num, desc, query_text, vector_type=None):
        """Run a vector search test and check if email_id appears."""
        # Build a fake args object for cmd_search
        search_args = argparse.Namespace(
            account_id=account_id,
            query=query_text,
            type=vector_type,
            filter=None,
        )
        try:
            vectors = cmd_search(search_args, return_results=True)
            found_ids = [v.get("metadata", {}).get("emailId") for v in (vectors or [])]
            passed = email_id in found_ids
        except Exception as e:
            passed = False
            found_ids = []
            print(f"  (error: {e})")

        suffix = ""
        if not passed:
            suffix = f" ({email_id} not in results)"
        type_str = f" --type {vector_type}" if vector_type else ""
        status = "PASS" if passed else "FAIL"
        line = f"[{status}] {num:>2}. search: text={query_text!r}{type_str}{suffix}"
        print(line)
        results.append(passed)

    def run_query_test(num, desc, query_filter):
        """Run an Email/query Lambda test and check if email_id appears."""
        try:
            result = invoke_query(account_id, query_filter)
            if result is None:
                passed = False
                ids = []
            else:
                ids, _ = result
                passed = email_id in ids
        except Exception as e:
            passed = False
            print(f"  (error: {e})")

        suffix = ""
        if not passed:
            suffix = f" ({email_id} not in results)"
        filter_str = json.dumps(query_filter, separators=(",", ":"))
        status = "PASS" if passed else "FAIL"
        line = f"[{status}] {num:>2}. query: {filter_str}{suffix}"
        print(line)
        results.append(passed)

    def run_query_error_test(num, desc, query_filter, expected_error):
        """Run an Email/query Lambda test and check that it returns the expected error type."""
        try:
            result = invoke_query_raw(account_id, query_filter)
            if result is None:
                passed = False
            else:
                name, resp_args = result
                error_type = resp_args.get("type", "")
                passed = name == "error" and error_type == expected_error
                if not passed:
                    suffix = f" (got name={name}, type={error_type})"
        except Exception as e:
            passed = False
            suffix = f" (error: {e})"

        if passed:
            suffix = ""
        filter_str = json.dumps(query_filter, separators=(",", ":"))
        status = "PASS" if passed else "FAIL"
        line = f"[{status}] {num:>2}. query error={expected_error}: {filter_str}{suffix}"
        print(line)
        results.append(passed)

    # A. Vector search
    print("--- A. Vector search (S3 Vectors) ---")
    run_search_test(1, "text search", subject_words)
    run_search_test(2, "subject type", subject_words, vector_type="subject")
    run_search_test(3, "body type", subject_words, vector_type="body")
    print()

    # B. Email/query — text path
    print("--- B. Email/query — text path ---")
    run_query_test(4, "text filter", {"text": subject_words})
    run_query_test(5, "subject filter", {"subject": subject})
    if first_mailbox:
        run_query_test(6, "text + inMailbox", {"text": subject_words, "inMailbox": first_mailbox})
    else:
        print(f"[SKIP]  6. text + inMailbox (no mailbox)")
        results.append(True)
    if from_email:
        run_query_test(7, "text + from", {"text": subject_words, "from": from_email})
    else:
        print(f"[SKIP]  7. text + from (no from address)")
        results.append(True)
    if first_keyword:
        run_query_test(8, "text + hasKeyword", {"text": subject_words, "hasKeyword": first_keyword})
    else:
        print(f"[SKIP]  8. text + hasKeyword (no keywords)")
        results.append(True)
    print()

    # C. Email/query — address path
    print("--- C. Email/query — address path ---")
    if from_email:
        run_query_test(9, "from filter", {"from": from_email})
        if first_mailbox:
            run_query_test(10, "from + inMailbox", {"from": from_email, "inMailbox": first_mailbox})
        else:
            print(f"[SKIP] 10. from + inMailbox (no mailbox)")
            results.append(True)
        if first_keyword:
            run_query_test(11, "from + hasKeyword", {"from": from_email, "hasKeyword": first_keyword})
        else:
            print(f"[SKIP] 11. from + hasKeyword (no keywords)")
            results.append(True)
    else:
        print(f"[SKIP]  9. from filter (no from address)")
        print(f"[SKIP] 10. from + inMailbox (no from address)")
        print(f"[SKIP] 11. from + hasKeyword (no from address)")
        results.extend([True, True, True])
    print()

    # D. Email/query — DynamoDB path
    print("--- D. Email/query — DynamoDB path ---")
    if first_mailbox:
        run_query_test(12, "inMailbox", {"inMailbox": first_mailbox})
        if first_keyword:
            run_query_test(13, "inMailbox + hasKeyword", {"inMailbox": first_mailbox, "hasKeyword": first_keyword})
        else:
            print(f"[SKIP] 13. inMailbox + hasKeyword (no keywords)")
            results.append(True)
    else:
        print(f"[SKIP] 12. inMailbox (no mailbox)")
        print(f"[SKIP] 13. inMailbox + hasKeyword (no mailbox)")
        results.extend([True, True])
    if first_keyword:
        run_query_test(14, "hasKeyword", {"hasKeyword": first_keyword})
    else:
        print(f"[SKIP] 14. hasKeyword (no keywords)")
        results.append(True)
    print()

    # E. Email/query — FilterOperator support
    print("--- E. Email/query — FilterOperator ---")
    if first_mailbox and first_keyword:
        run_query_test(15, "AND(inMailbox, hasKeyword)", {
            "operator": "AND",
            "conditions": [
                {"inMailbox": first_mailbox},
                {"hasKeyword": first_keyword},
            ],
        })
    else:
        print(f"[SKIP] 15. AND(inMailbox, hasKeyword) (need mailbox and keyword)")
        results.append(True)

    # OR with single condition — should flatten to just the inner condition
    run_query_test(16, "OR(text) single-condition flatten", {
        "operator": "OR",
        "conditions": [
            {"text": subject_words},
        ],
    })

    # aerc-style nested filter: AND(inMailboxOtherThan, OR(text))
    run_query_test(17, "AND(hasKeyword, OR(text)) aerc-style", {
        "operator": "AND",
        "conditions": [
            {"hasKeyword": first_keyword} if first_keyword else {},
            {
                "operator": "OR",
                "conditions": [
                    {"text": subject_words},
                ],
            },
        ],
    })

    # OR with multiple conditions — must return unsupportedFilter
    run_query_error_test(18, "OR multi → unsupportedFilter", {
        "operator": "OR",
        "conditions": [
            {"from": "alice@example.com"},
            {"from": "bob@example.com"},
        ],
    }, "unsupportedFilter")

    # NOT — must return unsupportedFilter
    run_query_error_test(19, "NOT → unsupportedFilter", {
        "operator": "NOT",
        "conditions": [
            {"from": "alice@example.com"},
        ],
    }, "unsupportedFilter")
    print()

    # Summary
    passed = sum(results)
    failed = len(results) - passed
    total = len(results)
    print(f"{total} tests: {passed} passed, {failed} failed")
    if failed > 0:
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Test script for the search indexing pipeline.",
    )
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # list-emails
    p = subparsers.add_parser("list-emails", help="List emails for an account")
    p.add_argument("account_id", help="Account ID")

    # show-email
    p = subparsers.add_parser("show-email", help="Show email details from DynamoDB")
    p.add_argument("account_id", help="Account ID")
    p.add_argument("email_id", help="Email ID")

    # index
    p = subparsers.add_parser("index", help="Send email for search indexing via SQS")
    p.add_argument("account_id", help="Account ID")
    p.add_argument("email_id", help="Email ID")

    # search
    p = subparsers.add_parser("search", help="Vector search via S3 Vectors")
    p.add_argument("account_id", help="Account ID")
    p.add_argument("query", help="Search query text")
    p.add_argument("--type", choices=["body", "subject"], help="Filter by vector type")
    p.add_argument("--filter", help="Additional metadata filter JSON")

    # list-tokens
    p = subparsers.add_parser("list-tokens", help="List TOK# entries from DynamoDB")
    p.add_argument("account_id", help="Account ID")
    p.add_argument("field", nargs="?", help="Token field (FROM/TO/CC/BCC)")
    p.add_argument("prefix", nargs="?", help="Token prefix to filter")

    # get-vectors
    p = subparsers.add_parser("get-vectors", help="Get stored vectors for an email")
    p.add_argument("account_id", help="Account ID")
    p.add_argument("email_id", help="Email ID")

    # query
    p = subparsers.add_parser("query", help="Invoke Email/query Lambda directly")
    p.add_argument("account_id", help="Account ID")
    p.add_argument("filter_json", help="JMAP filter as JSON string")

    # run-tests
    p = subparsers.add_parser("run-tests", help="Run automated test battery")
    p.add_argument("account_id", help="Account ID")
    p.add_argument("email_id", help="Email ID")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    commands = {
        "list-emails": cmd_list_emails,
        "show-email": cmd_show_email,
        "index": cmd_index,
        "search": cmd_search,
        "list-tokens": cmd_list_tokens,
        "get-vectors": cmd_get_vectors,
        "query": cmd_query_print,
        "run-tests": cmd_run_tests,
    }

    commands[args.command](args)


if __name__ == "__main__":
    main()
