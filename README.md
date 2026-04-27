# EMR Serverless Transaction Analytics (Java)

End-to-end Java Spark project running on **AWS EMR Serverless** — no cluster to manage, billed per-second, auto-stops after 15 minutes idle.

## Why EMR Serverless vs EMR on EC2?

| | EMR on EC2 | **EMR Serverless** |
|---|---|---|
| Provisioning | You size the cluster | AWS auto-scales workers |
| Idle cost | Pay for nodes sitting idle | Stops billing after IdleTimeoutMinutes |
| Best for | Long-running, steady workloads | Bursty/scheduled batch jobs |
| Setup time | ~7–10 minutes per cluster | ~1 minute (app stays warm) |

For a daily transaction batch like this one, Serverless is the cheaper and simpler choice.

## Answers to Your Questions

**Q: Do I need to upload the JAR to S3?**
Yes — but only for *your* code. The screenshot's `/usr/lib/spark/examples/jars/spark-examples.jar` is bundled on every EMR worker and needs no upload. SparkPi is the default "hello world" because it generates random points and needs no input data.

**Q: What about sample data?**
SparkPi needs none. *Your* job needs `transactions.json` uploaded to `s3://<bucket>/raw/date=YYYY-MM-DD/`.

## Project Layout

```
emr-serverless-java/
├── pom.xml                                          # Maven build with shade plugin
├── src/
│   ├── main/java/com/analytics/
│   │   └── TransactionAnalytics.java                # The Spark job
│   └── test/java/com/emr/analytics/
│       └── TransactionAnalyticsTest.java            # JUnit 5 tests
├── infra/
│   └── emr-serverless.yaml                          # CFN: app + IAM role
├── sample-data/
│   └── transactions.json                            # 15 rows, 2 planted anomalies
├── scripts/
│   └── build_and_submit.sh                          # Build + upload + submit
├── .github/workflows/
│   └── deploy.yml                                   # CI/CD
└── README.md
```

## End-to-End Manual Run

### 1. Build the fat JAR
```bash
mvn clean package
# Output: target/transaction-analytics-1.0.0.jar
```

The shade plugin produces a JAR with all dependencies bundled, but Spark itself is marked `provided` since it's already on EMR workers.

### 2. Deploy the EMR Serverless application
```bash
aws cloudformation deploy \
    --stack-name transaction-analytics-emr-serverless \
    --template-file infra/emr-serverless.yaml \
    --capabilities CAPABILITY_NAMED_IAM \
    --parameter-overrides \
        Environment=dev \
        DataBucket=my-data-bucket
```

### 3. Get the application ID and role ARN
```bash
APP_ID=$(aws cloudformation describe-stacks \
    --stack-name transaction-analytics-emr-serverless \
    --query "Stacks[0].Outputs[?OutputKey=='ApplicationId'].OutputValue" \
    --output text)

ROLE_ARN=$(aws cloudformation describe-stacks \
    --stack-name transaction-analytics-emr-serverless \
    --query "Stacks[0].Outputs[?OutputKey=='JobRoleArn'].OutputValue" \
    --output text)
```

### 4. Build, upload, and submit in one command
```bash
chmod +x scripts/build_and_submit.sh
./scripts/build_and_submit.sh my-data-bucket "$APP_ID" "$ROLE_ARN"
```

This script will:
1. Run `mvn package` to build the JAR
2. Upload the JAR to `s3://<bucket>/jars/`
3. Upload `sample-data/transactions.json` to `s3://<bucket>/raw/date=YYYY-MM-DD/`
4. Call `aws emr-serverless start-job-run` with the right parameters

### 5. Monitor the job
```bash
aws emr-serverless get-job-run \
    --application-id "$APP_ID" \
    --job-run-id <id-from-step-4>
```

Or in the AWS Console: **EMR Studio → Serverless → applications → Job runs**.

### 6. Inspect the output
```bash
aws s3 ls s3://my-data-bucket/curated/ --recursive
# curated/customer_daily/date=2026-04-25/part-*.parquet
# curated/anomalies/date=2026-04-25/part-*.parquet
# curated/category_trends/date=2026-04-25/country=US/part-*.parquet
```

Query directly with Athena once you create external tables pointing at the curated paths.

## What the Job Computes

Same as the Python version:

1. **Customer daily aggregates** — total spend, txn count, avg amount per customer per day
2. **Anomaly detection** — z-score against each customer's mean, flag z > 3.0
3. **Category trends** — volume and count by merchant category, country, date

The 15-row sample has two planted anomalies that should appear in the output:
- `txn_004`: $4,850 electronics purchase by cust_1001 (rest of their spend is $10–90)
- `txn_014`: $12,500 electronics purchase by cust_1005

## CloudFormation Highlights

- **`AWS::EMRServerless::Application`** — the long-lived "application" container. Jobs run inside it.
- **`InitialCapacity`** — pre-warmed driver + 4 executors so jobs start in ~1 minute instead of cold-starting.
- **`MaximumCapacity`** — caps the application at 200 vCPU / 800GB RAM regardless of how many jobs run concurrently.
- **`AutoStopConfiguration.IdleTimeoutMinutes: 15`** — stops billing 15 minutes after the last job finishes.
- **IAM role** is assumed by the `emr-serverless.amazonaws.com` service principal (not `ec2.amazonaws.com` like the EMR on EC2 version).

## GitHub Actions Pipeline

Three jobs: **build-and-test** (Maven test + package), **validate-cfn** (cfn-lint), and **deploy** (deploys CFN, uploads JAR, submits job). Uses OIDC federation — no long-lived AWS keys in GitHub.

Required GitHub secrets:
- `AWS_ACCOUNT_ID`
- `DATA_BUCKET`

The IAM role `GitHubActionsEMRServerlessRole` needs trust policy for `token.actions.githubusercontent.com` and permissions for `cloudformation:*`, `iam:*` (scoped to your role names), `s3:*` on your bucket, and `emr-serverless:StartJobRun`.

## Local Testing

```bash
mvn test
```

Runs JUnit 5 tests against a `local[2]` Spark session using `sample-data/transactions.json`. The `--add-opens` flags in the surefire config are required for Spark on JDK 11+ to access internal `java.base` modules.

## Cost Comparison (rough order of magnitude)

For a 5-minute daily batch processing 1M rows:
- **EMR on EC2** (3 m5.2xlarge running 24/7): ~$300/month
- **EMR Serverless** (5 min × 30 days × 16 vCPU): ~$15/month

Serverless wins decisively for short-duration scheduled work.
