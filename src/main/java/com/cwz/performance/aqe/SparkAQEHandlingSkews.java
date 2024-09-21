package com.cwz.performance.aqe;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions.*;

import java.util.Scanner;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.expr;

public class SparkAQEHandlingSkews {

    public static void main(String[] args) {
        SparkSession spark=SparkSession.builder()
                .appName("Spark AQE - Handling Skews ")
                .master("local[4]")
                .config("spark.dynamicAllocation.enabled","false")
                .config("spark.sql.adaptive.enabled","false")
                .config("spark.sql.adaptive.skewJoin.enabled","false")
                .getOrCreate();


//        Generate Product DF

        Dataset<Row> productsDF=spark.range(1,2000001) //Generate a range with id
                .select(
                        col("id").alias("ProductID"),
                                expr("ROUND(RAND() *100,2)").alias("Price")
                );

        productsDF.show();

        // Generate Sales DataFrame
        Dataset<Row> salesDF = spark.range(1, 100000001)  // Generates a range with 'id' column
                .select(
                        col("id").alias("SalesId"),
                        // ProductId - 70% values will be 1
                        expr(" CASE WHEN RAND() < 0.7 THEN 1 ELSE CAST(RAND() * 2000000 AS INT) END").alias("ProductId"),
                        // QuantitySold - Random
                        expr("CAST(RAND() * 10 AS INTEGER)").alias("QuantitySold"),
                        // SalesDate - Random date in the last 365 days
                        expr("DATE_ADD(CURRENT_DATE(), - CAST(RAND() * 365 AS INT))").alias("SalesDate")
                );

        salesDF.show();

        // Create temporary views for SQL operations
        productsDF.createOrReplaceTempView("Products");
        salesDF.createOrReplaceTempView("Sales");

        // First SQL Query - Product Count

        String productCountQuery=" SELECT ProductId, COUNT(*) AS ProductCount\n" +
                "                FROM Sales\n" +
                "                GROUP BY ProductId\n" +
                "                ORDER BY ProductCount DESC";
        spark.sql(productCountQuery).show();


        String perDaySalesQuery=" SELECT s.SalesDate, SUM(p.Price * s.QuantitySold) AS SalesAmount\n" +
                "                FROM Sales s\n" +
                "                JOIN Products p ON p.ProductId = s.ProductId\n" +
                "                GROUP BY s.SalesDate\n" +
                "                ORDER BY SalesAmount DESC";

        spark.sql(perDaySalesQuery).show();

        spark.conf().set("spark.sql.adaptive.enabled", "true");
//        spark.conf().set("spark.dynamicAllocation.enabled","true");
        spark.conf().set("spark.sql.adaptive.skewJoin.enabled","true");
        System.out.println("----------After AQE Enabled -------------");
        spark.sql(perDaySalesQuery).show();



        try (final var scanner = new Scanner(System.in)) {
            scanner.nextLine();
        }
    }
}
