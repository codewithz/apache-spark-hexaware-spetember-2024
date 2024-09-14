package com.cwz;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import org.apache.spark.sql.functions.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import static org.apache.spark.sql.functions.*;

public class DataframesOps {

    public static void main(String[] args) {

        SparkSession spark=SparkSession.builder()
                .appName("DataFrame/Dataset Operations ")
                .master("local[4]")
                .getOrCreate();


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

        Dataset<Row> yellowTaxiDF=spark
                .read()
                .option("header","true")
//                                        .option("inferSchema","true")
                .schema(yellowTaxiSchema)
//                .option("mode","FAILFAST") //DROPMALFORMED |FAILFAST |PERMISSIVE
                .csv(filePath);


        yellowTaxiDF.printSchema();
        yellowTaxiDF.show(false);

//        Get some analyzed Dataframes

        Dataset<Row> yellowTaxiAnalyzedDF=yellowTaxiDF.describe("passenger_count","trip_distance");
        yellowTaxiAnalyzedDF.show();

////        Accuracy Check in my dataset
////        1: Filter Inaccurate Data [passenger count <0 trip_distance <=0]
//
//        System.out.println("Before Applying Filter :"+yellowTaxiDF.count());
//        yellowTaxiDF=yellowTaxiDF
//                        .where("passenger_count>0")
//                       .filter(col("trip_distance").gt(0.0));
//
//        System.out.println("After Applying Filter :"+yellowTaxiDF.count());
//
////        Completeness Check
////        2.a -> Drop rows with null
////            yellowTaxiDF=yellowTaxiDF.na().drop("all");
//
////        2.b -> Replace with Default Value
        Map<String,Object> defaultValuesMap=new HashMap<>();
        defaultValuesMap.put("payment_type",5);
        defaultValuesMap.put("RateCodeId",1);

//        yellowTaxiDF=yellowTaxiDF.na().fill(defaultValuesMap);
//        System.out.println("After Applying Completeness Check :"+yellowTaxiDF.count());
//
////        3. Drop Duplicates
//
//        yellowTaxiDF=yellowTaxiDF.dropDuplicates();
//        System.out.println("After Dropping Duplicates:"+yellowTaxiDF.count());
//
////        4.Timeliness Check -- Removing the data out of bounds
//
//        yellowTaxiDF=yellowTaxiDF.where ("lpep_pickup_datetime >= '2022-10-01'" +
//                "    AND lpep_dropoff_datetime < '2022-11-01'");

//        ----If applied together -----

        // Apply operations: filter, drop nulls, fill defaults, remove duplicates, filter by date
        yellowTaxiDF = yellowTaxiDF
                // Filter rows where passenger_count > 0
                .where("passenger_count > 0")

                // Filter rows where trip_distance > 0.0
                .filter(col("trip_distance").gt(0.0))

                // Drop rows where all values are null
                .na().drop("all")

                // Fill missing values using the default value map
                .na().fill(defaultValuesMap)

                // Drop duplicate rows
                .dropDuplicates()

                // Filter rows based on pickup and dropoff datetime
                .where("lpep_pickup_datetime >= '2022-10-01' AND lpep_dropoff_datetime < '2022-11-01'");

        // Show the result after all operations
        yellowTaxiDF.show(false);


//        Selecting limited columns

        yellowTaxiDF=yellowTaxiDF.select(
                col("VendorId"),
                col("passenger_count").cast("int"),
                col("trip_distance").alias("TripDistance"),
                col("lpep_pickup_datetime"),          // Select lpep_pickup_datetime
                col("lpep_dropoff_datetime"),         // Select lpep_dropoff_datetime
                col("PUlocationID"),                  // Select PUlocationID
                col("DOlocationID"),                  // Select DOlocationID
                col("RatecodeID"),                    // Select RatecodeID
                col("total_amount"),                  // Select total_amount
                col("payment_type")


        );

        yellowTaxiDF.printSchema();

//        Renaming the columns

        yellowTaxiDF=yellowTaxiDF
                .withColumnRenamed("passenger_count","PassengerCount")
                .withColumnRenamed("lpep_pickup_datetime", "PickupTime")
                .withColumnRenamed("lpep_dropoff_datetime", "DropTime")
                .withColumnRenamed("PUlocationID", "PickupLocationId")
                .withColumnRenamed("DOlocationID", "DropLocationId")
                .withColumnRenamed("total_amount", "TotalAmount")
                .withColumnRenamed("payment_type", "PaymentType");

        yellowTaxiDF.printSchema();

//        Add column by name of TripYear

        yellowTaxiDF=yellowTaxiDF.withColumn("TripYear",year(col("PickupTime")));

        yellowTaxiDF=yellowTaxiDF.select(
                col("*"),
                month(col("PickupTime")).alias("TripMonth"),
                dayofmonth(col("PickupTime")).alias("TripDay")
        );

//        yellowTaxiDF=yellowTaxiDF.withColumn("TripTimeInMinutes",
//                round(
//                        (unix_timestamp(col("DropTime"))
//                                .minus(
//                                        unix_timestamp(col("PickupTime")
//                                )
//                        ))
//                                .divide(60),2
//                ));

        Column tripTimeInSecondsExpr=unix_timestamp(col("DropTime")).minus(unix_timestamp(col("PickupTime")));
        Column tripTimeInMinutesExpr=round(tripTimeInSecondsExpr.divide(60),2);

        yellowTaxiDF=yellowTaxiDF.withColumn("TripTimeInMinutes",tripTimeInMinutesExpr);

        Column tripTypeColumn=when(col("RateCodeId").equalTo(6),"SharedTrip")
                .otherwise("SoloTrip");
        yellowTaxiDF=yellowTaxiDF.withColumn("TripType",tripTypeColumn);

        yellowTaxiDF.printSchema();

        yellowTaxiDF.show(5);

        Dataset<Row> yellowTaxiDFReport=yellowTaxiDF
                .groupBy("PickupLocationId","DropLocationId")
                .agg(
                        avg("TripTimeInMinutes").alias("AvgTripTime"),
                        sum("TotalAmount").alias("SumAmount")
                )
                .orderBy(col("PickupLocationId").desc());

        yellowTaxiDFReport.show();




        try (final var scanner = new Scanner(System.in)) {
            scanner.nextLine();
        }

    }
}
