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

public class SparkAQEDynamicCoalescing {


    public static void main(String[] args) {
        SparkSession spark=SparkSession.builder()
                .appName("Spark AQE - Dynamic Coalescing ")
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

        System.out.println("Partitions :"+yellowTaxiDF.rdd().getNumPartitions());

        spark.conf().set("spark.sql.shuffle.partitions", 20);

        Dataset<Row> yellowTaxiGroupedDF = yellowTaxiDF
                .groupBy("VendorId", "payment_type")
                .agg(sum("total_amount"));

        yellowTaxiGroupedDF.show();

        // Print the number of partitions before enabling AQE
        System.out.println("Partitions before AQE = " + yellowTaxiGroupedDF.rdd().getNumPartitions());

        // Get partition stats before enabling AQE
        getDataFrameStats(yellowTaxiGroupedDF, "VendorId").show();


        // Enable AQE and coalescing
        spark.conf().set("spark.sql.adaptive.enabled", "true");
        spark.conf().set("spark.sql.adaptive.coalescePartitions.enabled", "true");


     yellowTaxiGroupedDF = yellowTaxiDF
                .groupBy("VendorId", "payment_type")
                .agg(sum("total_amount"));

        yellowTaxiGroupedDF.show();

        // Print the number of partitions after enabling AQE
        System.out.println("Partitions after AQE = " + yellowTaxiGroupedDF.rdd().getNumPartitions());

        // Get partition stats before enabling AQE
        getDataFrameStats(yellowTaxiGroupedDF, "VendorId").show();


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
