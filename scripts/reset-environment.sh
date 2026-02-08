#!/usr/bin/env bash
# Reset JMAP Service Email environment by clearing all data and re-initializing

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Usage
usage() {
    echo "Usage: $0 <env> [--dry-run]"
    echo ""
    echo "Arguments:"
    echo "  env       Environment to reset (test or prod)"
    echo "  --dry-run Show what would be done without executing"
    exit 1
}

# Parse arguments
ENV="${1:-}"
DRY_RUN=false

if [ -z "$ENV" ]; then
    usage
fi

if [ "$ENV" != "test" ] && [ "$ENV" != "prod" ]; then
    echo -e "${RED}Error: ENV must be 'test' or 'prod'${NC}"
    usage
fi

if [ "${2:-}" = "--dry-run" ]; then
    DRY_RUN=true
fi

# Ensure AWS_PROFILE is set
if [ -z "${AWS_PROFILE:-}" ]; then
    export AWS_PROFILE=ses-mail
fi

# Production safety check
if [ "$ENV" = "prod" ] && [ "$DRY_RUN" = false ]; then
    echo -e "${RED}WARNING: You are about to PERMANENTLY DELETE ALL DATA in PRODUCTION${NC}"
    echo "This will:"
    echo "  - Delete all S3 vectors"
    echo "  - Delete all DynamoDB records"
    echo "  - Reset all accounts to default mailboxes only"
    echo ""
    read -p "Type 'prod' to confirm: " confirm1
    if [ "$confirm1" != "prod" ]; then
        echo "Aborted."
        exit 1
    fi
    read -p "Type 'prod' again to confirm: " confirm2
    if [ "$confirm2" != "prod" ]; then
        echo "Aborted."
        exit 1
    fi
    echo ""
fi

# Resource names
S3_BUCKET="jmap-service-email-${ENV}-search-vectors"
DYNAMODB_TABLE="jmap-service-email-data-${ENV}"
LAMBDA_NAME="jmap-service-email-${ENV}-account-init"

echo "Environment: $ENV"
echo "S3 Bucket: $S3_BUCKET"
echo "DynamoDB Table: $DYNAMODB_TABLE"
echo "Lambda: $LAMBDA_NAME"
echo ""

# Step 1: Discover account IDs before deletion
echo "Step 1: Discovering account IDs..."
if [ "$DRY_RUN" = true ]; then
    echo -e "${YELLOW}[DRY-RUN]${NC} Would scan DynamoDB for account IDs"
    ACCOUNT_IDS=()
else
    # Query for all items with pk starting with "account#"
    # Extract unique account IDs from pk values
    ACCOUNT_IDS=($(aws dynamodb scan \
        --table-name "$DYNAMODB_TABLE" \
        --projection-expression pk \
        --filter-expression "begins_with(pk, :prefix)" \
        --expression-attribute-values '{":prefix":{"S":"account#"}}' \
        --output text \
        --query 'Items[*].pk.S' \
        2>/dev/null | tr '\t' '\n' | cut -d'#' -f2 | sort -u || echo ""))

    if [ ${#ACCOUNT_IDS[@]} -eq 0 ]; then
        echo -e "${YELLOW}No accounts found in DynamoDB${NC}"
    else
        echo -e "${GREEN}Found ${#ACCOUNT_IDS[@]} account(s):${NC}"
        for account in "${ACCOUNT_IDS[@]}"; do
            echo "  - $account"
        done
    fi
fi
echo ""

# Step 2: Delete S3 Vector indexes
echo "Step 2: Deleting S3 vector indexes..."
if [ "$DRY_RUN" = true ]; then
    echo -e "${YELLOW}[DRY-RUN]${NC} Would delete all indexes from vector bucket: ${S3_BUCKET}"
else
    # List all indexes in the vector bucket
    INDEXES_JSON=$(aws s3vectors list-indexes \
        --vector-bucket-name "$S3_BUCKET" \
        --output json 2>/dev/null || echo '{"indexes":[]}')

    INDEX_COUNT=$(echo "$INDEXES_JSON" | jq '.indexes | length')

    if [ "$INDEX_COUNT" -eq 0 ]; then
        echo -e "${GREEN}No indexes found in vector bucket${NC}"
    else
        echo "Found $INDEX_COUNT index(es), deleting..."

        # Delete each index
        echo "$INDEXES_JSON" | jq -r '.indexes[].indexName' | while read -r index_name; do
            if [ -n "$index_name" ]; then
                echo "  Deleting index: $index_name"
                aws s3vectors delete-index \
                    --vector-bucket-name "$S3_BUCKET" \
                    --index-name "$index_name" \
                    2>/dev/null || echo "    ${YELLOW}Warning: Failed to delete index${NC}"
            fi
        done

        echo -e "${GREEN}All vector indexes deleted${NC}"
    fi
fi
echo ""

# Step 3: Delete DynamoDB records
echo "Step 3: Deleting DynamoDB records..."
if [ "$DRY_RUN" = true ]; then
    echo -e "${YELLOW}[DRY-RUN]${NC} Would scan and delete all items from ${DYNAMODB_TABLE}"
else
    # Scan and delete in batches
    ITEMS_DELETED=0
    BATCH_SIZE=25

    # Scan for all items (we need pk and sk to delete)
    SCAN_OUTPUT=$(mktemp)
    aws dynamodb scan \
        --table-name "$DYNAMODB_TABLE" \
        --projection-expression "pk, sk" \
        --output json > "$SCAN_OUTPUT" 2>/dev/null || true

    TOTAL_ITEMS=$(jq '.Items | length' "$SCAN_OUTPUT")

    if [ "$TOTAL_ITEMS" -eq 0 ]; then
        echo -e "${GREEN}DynamoDB table already empty${NC}"
    else
        echo "Deleting $TOTAL_ITEMS item(s)..."

        # Process items in batches
        for ((i=0; i<$TOTAL_ITEMS; i+=$BATCH_SIZE)); do
            BATCH=$(jq -c ".Items[$i:$((i+$BATCH_SIZE))]" "$SCAN_OUTPUT")
            BATCH_LENGTH=$(echo "$BATCH" | jq 'length')

            # Build batch delete request
            DELETE_REQUESTS=$(echo "$BATCH" | jq -c '[.[] | {DeleteRequest: {Key: {pk: .pk, sk: .sk}}}]')
            REQUEST_ITEMS=$(jq -n --arg table "$DYNAMODB_TABLE" --argjson requests "$DELETE_REQUESTS" '{($table): $requests}')

            # Execute batch delete
            aws dynamodb batch-write-item \
                --request-items "$REQUEST_ITEMS" \
                >/dev/null 2>&1 || echo "  Warning: Some items may have failed to delete"

            ITEMS_DELETED=$((ITEMS_DELETED + BATCH_LENGTH))
            echo "  Deleted $ITEMS_DELETED / $TOTAL_ITEMS items"
        done

        rm -f "$SCAN_OUTPUT"
        echo -e "${GREEN}DynamoDB table cleared (${ITEMS_DELETED} items deleted)${NC}"
    fi
fi
echo ""

# Step 4: Re-initialize accounts
if [ ${#ACCOUNT_IDS[@]} -eq 0 ]; then
    echo "Step 4: No accounts to re-initialize"
else
    echo "Step 4: Re-initializing accounts..."
    for account in "${ACCOUNT_IDS[@]}"; do
        if [ "$DRY_RUN" = true ]; then
            echo -e "${YELLOW}[DRY-RUN]${NC} Would invoke $LAMBDA_NAME for account: $account"
        else
            # Build synthetic SQS event payload
            PAYLOAD=$(cat <<EOF
{
  "Records": [
    {
      "messageId": "reset-$(date +%s)",
      "body": "{\"eventType\":\"account.created\",\"occurredAt\":\"$(date -u +"%Y-%m-%dT%H:%M:%SZ")\",\"accountId\":\"$account\"}"
    }
  ]
}
EOF
)

            echo "  Invoking Lambda for account: $account"
            RESULT=$(aws lambda invoke \
                --function-name "$LAMBDA_NAME" \
                --payload "$PAYLOAD" \
                --cli-binary-format raw-in-base64-out \
                /dev/stdout \
                2>/dev/null | head -1)

            # Check for errors in response
            if echo "$RESULT" | jq -e '.errorMessage' >/dev/null 2>&1; then
                ERROR=$(echo "$RESULT" | jq -r '.errorMessage')
                echo -e "    ${RED}Error: $ERROR${NC}"
            else
                echo -e "    ${GREEN}Success${NC}"
            fi
        fi
    done
fi
echo ""

# Summary
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Summary:"
if [ "$DRY_RUN" = true ]; then
    echo -e "${YELLOW}DRY-RUN MODE - No changes made${NC}"
else
    echo -e "${GREEN}Environment reset complete${NC}"
fi
echo "  Environment: $ENV"
echo "  Accounts found: ${#ACCOUNT_IDS[@]}"
echo ""
echo "Verification commands:"
echo "  aws s3vectors list-indexes --vector-bucket-name ${S3_BUCKET}"
echo "  aws dynamodb scan --table-name ${DYNAMODB_TABLE} --select COUNT"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
