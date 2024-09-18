package com.cwz;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Scanner;

public class SparkSQLWithMultiDatasets {

    public static void main(String[] args) {
        SparkSession spark=SparkSession.builder()
                .appName("Spark With SQL Operations ")
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



        System.out.println("------ Adding taxi Zones ----------------------");
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


        Dataset<Row> joinedDF=yellowTaxiDF
                                .join(
                                        taxiZonesDF,
                                        yellowTaxiDF.col("PULocationID").equalTo(taxiZonesDF.col("LocationID")),
                                        "inner"
                                ); //jointypes  -->left,leftouter,right,rightouter,full

        joinedDF.printSchema();
//        joinedDF.show();

//        ---------------------------------------------------

        yellowTaxiDF.createOrReplaceTempView("YellowTaxis");
        taxiZonesDF.createOrReplaceTempView("TaxiZones");

//        ---------------------------------------------------
//        Question --> Find all LocationIds in TaxiZones, from where no pickups have happened

        Dataset<Row> resultDF = spark.sql(
                "SELECT DISTINCT tz.* " +
                        "FROM TaxiZones tz " +
                        "LEFT JOIN YellowTaxis yt ON yt.PULocationID = tz.LocationId " +
                        "WHERE yt.PULocationID IS NULL"
        );

        resultDF.show();

//        Set Operations -- UNION, UNION ALL, INTERSECT, EXCEPT


        String cabsFilePath="C:\\Spark\\DataFiles\\Cabs.csv";

        Dataset<Row> cabsDF=spark
                .read()
                .option("header","true")
                .option("inferSchema","true")
                .csv(cabsFilePath);

        cabsDF.createOrReplaceTempView("Cabs");

        String driversFilePath="C:\\Spark\\DataFiles\\Drivers.csv";

        Dataset<Row> driversDF=spark
                .read()
                .option("header","true")
                .option("inferSchema","true")
                .csv(driversFilePath);


        driversDF.createOrReplaceTempView("Drivers");

        driversDF.show(100);

//        Count of all the drivers

        String exceptQuery="  (\n" +
                "        SELECT Name \n" +
                "        FROM Cabs\n" +
                "        WHERE LicenseType = 'OWNER MUST DRIVE'\n" +
                "    )\n" +
                "\n" +
                "    EXCEPT\n" +
                "\n" +
                "    (\n" +
                "        SELECT Name\n" +
                "        FROM Drivers\n" +
                "    )";

        Dataset<Row> unregisteredDrivers=spark.sql(exceptQuery);

        unregisteredDrivers.show(false);
        long count=unregisteredDrivers.count();
        System.out.println("Total Count:"+count);



        try (final var scanner = new Scanner(System.in)) {
            scanner.nextLine();
        }

    }
}
