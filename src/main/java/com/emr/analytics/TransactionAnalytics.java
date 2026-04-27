package com.emr.analytics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.*;

/**
 * Transaction Analytics Spark job for EMR Serverless.
 *
 * Reads raw transaction JSON from S3 and produces three curated datasets:
 *   1. Daily customer aggregates (total spend, txn count, avg amount)
 *   2. Anomaly detection via z-score (transactions > 3 stddev from customer mean)
 *   3. Merchant category trends by date and country
 *
 * Submitted to EMR Serverless as:
 *   spark-submit --class com.emr.analytics.TransactionAnalytics
 *                s3://bucket/jars/transaction-analytics-1.0.0.jar
 *                s3://bucket/raw/
 *                s3://bucket/curated/
 */
public class TransactionAnalytics {

    public static final StructType TRANSACTION_SCHEMA = new StructType()
        .add("transaction_id", DataTypes.StringType, false)
        .add("customer_id", DataTypes.StringType, false)
        .add("merchant_category", DataTypes.StringType, true)
        .add("amount", DataTypes.DoubleType, false)
        .add("currency", DataTypes.StringType, true)
        .add("transaction_ts", DataTypes.TimestampType, false)
        .add("country", DataTypes.StringType, true);

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: TransactionAnalytics <input_path> <output_path>");
            System.exit(1);
        }
        String inputPath = args[0];
        String outputPath = args[1];

        SparkSession spark = SparkSession.builder()
            .appName("TransactionAnalytics")
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .enableHiveSupport()
            .getOrCreate();

        try {
            Dataset<Row> transactions = readTransactions(spark, inputPath).cache();

            Dataset<Row> customerDaily = computeCustomerDaily(transactions);
            Dataset<Row> anomalies = computeAnomalies(transactions, 3.0);
            Dataset<Row> categoryTrends = computeCategoryTrends(transactions);

            customerDaily.write()
                .mode("overwrite")
                .partitionBy("date")
                .parquet(outputPath + "/customer_daily/");

            anomalies.write()
                .mode("overwrite")
                .partitionBy("date")
                .parquet(outputPath + "/anomalies/");

            categoryTrends.write()
                .mode("overwrite")
                .partitionBy("date", "country")
                .parquet(outputPath + "/category_trends/");
        } finally {
            spark.stop();
        }
    }

    /** Read raw JSON, filter invalid amounts, derive a date column for partitioning. */
    public static Dataset<Row> readTransactions(SparkSession spark, String inputPath) {
        return spark.read()
            .schema(TRANSACTION_SCHEMA)
            .json(inputPath)
            .filter(col("amount").gt(0))
            .withColumn("date", date_format(col("transaction_ts"), "yyyy-MM-dd"));
    }

    /** One row per customer per day with spend aggregates. */
    public static Dataset<Row> computeCustomerDaily(Dataset<Row> df) {
        return df.groupBy("customer_id", "date")
            .agg(
                sum("amount").alias("total_spend"),
                count(lit(1)).alias("txn_count"),
                avg("amount").alias("avg_txn_amount")
            );
    }

    /** Flag transactions exceeding zThreshold stddevs from each customer's mean. */
    public static Dataset<Row> computeAnomalies(Dataset<Row> df, double zThreshold) {
        Dataset<Row> customerStats = df.groupBy("customer_id")
            .agg(
                avg("amount").alias("mean_amount"),
                stddev("amount").alias("stddev_amount")
            );

        return df.join(customerStats, "customer_id")
            .withColumn(
                "z_score",
                col("amount").minus(col("mean_amount")).divide(col("stddev_amount"))
            )
            .filter(col("z_score").gt(zThreshold))
            .select("transaction_id", "customer_id", "amount", "z_score", "date");
    }

    /** Volume and count per merchant category, country, and date. */
    public static Dataset<Row> computeCategoryTrends(Dataset<Row> df) {
        return df.groupBy("date", "merchant_category", "country")
            .agg(
                sum("amount").alias("category_volume"),
                count(lit(1)).alias("txn_count")
            );
    }
}
