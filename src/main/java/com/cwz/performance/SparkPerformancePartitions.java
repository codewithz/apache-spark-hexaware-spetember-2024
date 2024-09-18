package com.cwz.performance;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Scanner;

public class SparkPerformancePartitions {

    public static void main(String[] args) {
        SparkSession spark=SparkSession.builder()
                .appName("Spark Performance - Partitions ")
                .master("local[4]")
                .getOrCreate();

        JavaSparkContext context=new JavaSparkContext(spark.sparkContext());

        System.out.println("Default Parallelism: "+context.defaultParallelism());
        System.out.println("Default Min Partitions: "+context.defaultMinPartitions());

        StructType taxiZonesSchema = new StructType()
                .add("LocationID", DataTypes.IntegerType, true)
                .add("Borough", DataTypes.StringType, true)
                .add("Zone", DataTypes.StringType, true)
                .add("ServiceZone", DataTypes.StringType, true);

        Dataset<Row> taxiZonesDF=spark
                .read()
                .option("header","true")
                .option("inferSchema","true")
                .schema(taxiZonesSchema)
                .csv("C:\\Spark\\new-data\\TaxiZones.csv");

        System.out.println("Partitions :"+taxiZonesDF.rdd().getNumPartitions());

        System.out.println("Record Counts : "+taxiZonesDF.count());

        System.out.println("------------ For a larger Dataset ------------------");

        //       Load from Data file -- csv
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


        spark.conf().set("spark.sql.files.maxPartitionBytes","64m");

        Dataset<Row> yellowTaxiDF=spark
                .read()
                .option("header","true")
                .schema(yellowTaxiSchema)
                .csv(filePath);

        System.out.println("Partitions :"+yellowTaxiDF.rdd().getNumPartitions());

        System.out.println("Record Counts : "+yellowTaxiDF.count());

        try (final var scanner = new Scanner(System.in)) {
            scanner.nextLine();
        }


    }
}
