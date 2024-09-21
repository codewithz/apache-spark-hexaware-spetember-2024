package com.cwz.performance.aqe;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Scanner;

import static org.apache.spark.sql.functions.*;

public class SparkAQEDynamicJoinStrategy {


    public static void main(String[] args) {
        SparkSession spark=SparkSession.builder()
                .appName("Spark AQE - Dynamic Join Strategy ")
                .master("local[4]")
                .config("spark.dynamicAllocation.enabled","false")
                .config("spark.sql.adaptive.enabled","false")
                .getOrCreate();



        String filePath="C:\\Spark\\DataFiles\\YellowTaxis_202210.csv";

        StructType yellowTaxiSchema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("VendorId", DataTypes.IntegerType, true),
                DataTypes.createStructField("lpep_pickup_datetime", DataTypes.TimestampType, true),
                DataTypes.createStructField("lpep_dropoff_datetime", DataTypes.TimestampType, true),
                DataTypes.createStructField("passenger_count", DataTypes.DoubleType, true),
                DataTypes.createStructField("trip_distance", DataTypes.DoubleType, true),
                DataTypes.createStructField("RatecodeID", DataTypes.DoubleType, true),
                DataTypes.createStructField("store_and_fwd_flag", DataTypes.StringType, true),
                DataTypes.createStructField("PULocationID", DataTypes.IntegerType, true),
                DataTypes.createStructField("DOLocationID", DataTypes.IntegerType, true),
                DataTypes.createStructField("payment_type", DataTypes.IntegerType, true),
                DataTypes.createStructField("fare_amount", DataTypes.DoubleType, true),
                DataTypes.createStructField("extra", DataTypes.DoubleType, true),
                DataTypes.createStructField("mta_tax", DataTypes.DoubleType, true),
                DataTypes.createStructField("tip_amount", DataTypes.DoubleType, true),
                DataTypes.createStructField("tolls_amount", DataTypes.DoubleType, true),
                DataTypes.createStructField("improvement_surcharge", DataTypes.DoubleType, true),
                DataTypes.createStructField("total_amount", DataTypes.DoubleType, true),
                DataTypes.createStructField("congestion_surcharge", DataTypes.DoubleType, true),
                DataTypes.createStructField("airport_fee", DataTypes.DoubleType, true)
        });

        // Print the schema for verification
        System.out.println(yellowTaxiSchema.prettyJson());


//        spark.conf().set("spark.sql.files.maxPartitionBytes","64m");

        Dataset<Row> yellowTaxiDF=spark
                .read()
                .option("header","true")
                .schema(yellowTaxiSchema)
                .csv(filePath);


//        -- yellowTaxiDF --> 3675412

        yellowTaxiDF.createOrReplaceTempView("YellowTaxis1");
        yellowTaxiDF.createOrReplaceTempView("YellowTaxis2");

        String joinQuery ="SELECT *\n" +
                "\n" +
                "FROM YellowTaxis1 yt1\n" +
                "\n" +
                "    JOIN YellowTaxis2 yt2 ON yt1.PULocationId = yt2.DOLocationId\n" +
                "    \n" +
                "    WHERE yt1.RateCodeId = 4";

        spark.sql(joinQuery).show();

        // Enable AQE and coalescing
        spark.conf().set("spark.sql.adaptive.enabled", "true");
        spark.conf().set("spark.sql.adaptive.autoBroadcastJoinThreshold", "10m");

        spark.sql(joinQuery).show();



        try (final var scanner = new Scanner(System.in)) {
            scanner.nextLine();
        }
    }


    // Method to get DataFrame statistics
    public static Dataset<Row> getDataFrameStats(Dataset<Row> dataFrame, String columnName) {
        return dataFrame
                // Get partition number for each record
                .withColumn("Partition Number", functions.spark_partition_id())

                // Group by partition and calculate stats for the specified column
                .groupBy("Partition Number")
                .agg(
                        count("*").alias("Record Count"),
                        min(columnName).alias("Min Column Value"),
                        max(columnName).alias("Max Column Value")
                )
                // Order the results by partition number
                .orderBy("Partition Number");
    }
}
