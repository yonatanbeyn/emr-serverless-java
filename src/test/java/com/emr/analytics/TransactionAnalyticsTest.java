package com.emr.analytics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;
import static org.junit.jupiter.api.Assertions.*;

class TransactionAnalyticsTest {

    private static SparkSession spark;
    private static Dataset<Row> sampleDf;

    @BeforeAll
    static void setUp() {
        spark = SparkSession.builder()
            .master("local[2]")
            .appName("transaction-analytics-tests")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.ui.enabled", "false")
            .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        sampleDf = spark.read()
            .schema(TransactionAnalytics.TRANSACTION_SCHEMA)
            .json("sample-data/transactions.json")
            .filter(col("amount").gt(0))
            .withColumn("date", date_format(col("transaction_ts"), "yyyy-MM-dd"))
            .cache();
    }

    @AfterAll
    static void tearDown() {
        if (spark != null) spark.stop();
    }

    @Test
    void customerDailyAggregatesAreCorrect() {
        List<Row> result = TransactionAnalytics.computeCustomerDaily(sampleDf).collectAsList();

        Map<String, Row> byCustomerDate = new HashMap<>();
        for (Row r : result) {
            byCustomerDate.put(r.getAs("customer_id") + "|" + r.getAs("date"), r);
        }

        // cust_1001 on 2024-01-15: 87.45 + 52.10 + 23.75 + 4850.00 + 9.99 = 5023.29 across 5 txns
        Row cust1001 = byCustomerDate.get("cust_1001|2024-01-15");
        assertEquals(5L, (Long) cust1001.getAs("txn_count"));
        assertEquals(5023.29, (double) cust1001.getAs("total_spend"), 0.01);

        // cust_1002 on 2024-01-15: 142.30 + 14.99 + 67.80 = 225.09 across 3 txns
        Row cust1002 = byCustomerDate.get("cust_1002|2024-01-15");
        assertEquals(3L, (Long) cust1002.getAs("txn_count"));
        assertEquals(225.09, (double) cust1002.getAs("total_spend"), 0.01);
    }

    @Test
    void anomalyDetectionFindsPlantedOutliers() {
        List<Row> anomalies = TransactionAnalytics.computeAnomalies(sampleDf, 3.0).collectAsList();
        boolean foundTxn004 = anomalies.stream()
            .anyMatch(r -> "txn_004".equals(r.getAs("transaction_id")));

        assertTrue(foundTxn004,
            "Expected txn_004 ($4,850 outlier for cust_1001) to be flagged");
    }

    @Test
    void categoryTrendsVolumeIsCorrect() {
        List<Row> result = TransactionAnalytics.computeCategoryTrends(sampleDf).collectAsList();

        Row usGroceries = result.stream()
            .filter(r -> "US".equals(r.getAs("country"))
                      && "groceries".equals(r.getAs("merchant_category")))
            .findFirst()
            .orElseThrow(() -> new AssertionError("US groceries row not found"));

        // 87.45 + 142.30 + 95.20 + 78.40 = 403.35
        assertEquals(403.35, (double) usGroceries.getAs("category_volume"), 0.01);
        assertEquals(4L, (Long) usGroceries.getAs("txn_count"));
    }

    @Test
    void noNegativeAmounts() {
        long negativeCount = sampleDf.filter("amount <= 0").count();
        assertEquals(0L, negativeCount);
    }
}
