#!/bin/bash
# build_and_submit.sh — End-to-end: build JAR, upload artifacts, run job
#
# Usage: ./build_and_submit.sh <bucket> <application-id> <job-role-arn>

set -euo pipefail

BUCKET=${1:?Usage: ./build_and_submit.sh <bucket> <app-id> <role-arn>}
APP_ID=${2:?Usage: ./build_and_submit.sh <bucket> <app-id> <role-arn>}
ROLE_ARN=${3:?Usage: ./build_and_submit.sh <bucket> <app-id> <role-arn>}

DATE=$(date +%Y-%m-%d)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}/.."

echo "===> Building fat JAR with Maven"
cd "${PROJECT_ROOT}"
mvn clean package -DskipTests

JAR_PATH="${PROJECT_ROOT}/target/transaction-analytics-1.0.0.jar"
if [[ ! -f "${JAR_PATH}" ]]; then
    echo "ERROR: JAR not built at ${JAR_PATH}"
    exit 1
fi

echo "===> Uploading JAR to s3://${BUCKET}/jars/"
aws s3 cp "${JAR_PATH}" "s3://${BUCKET}/jars/transaction-analytics-1.0.0.jar"

echo "===> Uploading sample data to s3://${BUCKET}/raw/date=${DATE}/"
aws s3 cp "${PROJECT_ROOT}/sample-data/transactions.json" \
    "s3://${BUCKET}/raw/date=${DATE}/transactions.json"

echo "===> Submitting EMR Serverless job"
JOB_RUN_ID=$(aws emr-serverless start-job-run \
    --application-id "${APP_ID}" \
    --execution-role-arn "${ROLE_ARN}" \
    --name "transaction-analytics-${DATE}" \
    --job-driver "{
        \"sparkSubmit\": {
            \"entryPoint\": \"s3://${BUCKET}/jars/transaction-analytics-1.0.0.jar\",
            \"entryPointArguments\": [
                \"s3://${BUCKET}/raw/\",
                \"s3://${BUCKET}/curated/\"
            ],
            \"sparkSubmitParameters\": \"--class com.emr.analytics.TransactionAnalytics --conf spark.executor.cores=4 --conf spark.executor.memory=8g --conf spark.driver.cores=2 --conf spark.driver.memory=4g\"
        }
    }" \
    --configuration-overrides "{
        \"monitoringConfiguration\": {
            \"s3MonitoringConfiguration\": {
                \"logUri\": \"s3://${BUCKET}/emr-logs/\"
            }
        }
    }" \
    --query 'jobRunId' --output text)

echo "===> Job submitted. ID: ${JOB_RUN_ID}"
echo "Tail logs with:"
echo "  aws emr-serverless get-job-run --application-id ${APP_ID} --job-run-id ${JOB_RUN_ID}"
echo "Or watch the AWS Console: EMR Studio -> Serverless -> ${APP_ID} -> Job runs"
