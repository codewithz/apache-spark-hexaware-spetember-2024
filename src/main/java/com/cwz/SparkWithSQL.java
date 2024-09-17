package com.cwz;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Scanner;

public class SparkWithSQL {

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


        yellowTaxiDF.printSchema();
        yellowTaxiDF.show(false);

        yellowTaxiDF.createOrReplaceTempView("YellowTaxis");

        Dataset<Row> outputDF=spark.sql("" +
                "Select * from YellowTaxis WHERE PULocationID=171");

        outputDF.show();

//        --------------------------------------------------------------------------------------

        Dataset<Row> greenTaxiDF=spark
                .read()
                .option("header","true")
                .option("inferSchema","true")
                .option("delimiter","\t")
                .csv("C:\\Spark\\new-data\\GreenTaxis_202210.csv");

        greenTaxiDF.createOrReplaceTempView("GreenTaxis");

        Dataset<Row> greenTaxiOutputDF=spark.sql("Select * from GreenTaxis");

        greenTaxiOutputDF.show();

        System.out.println("------------ Unioned Dataframe -------------");

        String unionedTaxiQuery="SELECT 'Yellow'                   AS TaxiType\n" +
                "\n" +
                "      , lpep_pickup_datetime      AS PickupTime\n" +
                "      , lpep_dropoff_datetime     AS DropTime\n" +
                "      , PULocationID              AS PickupLocationId\n" +
                "      , DOLocationID              AS DropLocationId      \n" +
                "FROM YellowTaxis\n" +
                "\n" +
                "UNION ALL\n" +
                "\n" +
                "SELECT 'Green'                    AS TaxiType\n" +
                "\n" +
                "      , lpep_pickup_datetime      AS PickupTime\n" +
                "      , lpep_dropoff_datetime     AS DropTime\n" +
                "      , PULocationID              AS PickupLocationId\n" +
                "      , DOLocationID              AS DropLocationId \n" +
                "FROM GreenTaxis";

        Dataset<Row> unionedTaxisDF=spark.sql(unionedTaxiQuery);

        unionedTaxisDF.show(100);

        unionedTaxisDF.createOrReplaceTempView("GreenYellowTaxis");

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


        taxiZonesDF.createOrReplaceTempView("TaxiZones");

        taxiZonesDF.count();

        spark.sql("Select * from TaxiZones").show();

//      New Data  -- https://tinyurl.com/yc5k4kn9
//        Question -- Figure out number of rides, grouped by Borough and type of Taxi

//        ------------+--------+----------+
//                |      Borough|TaxiType|TotalTrips|
//                +-------------+--------+----------+
//                |      Bronx|   Green|      1852|
//|                      Bronx|  Yellow|      4511|
//|                    Brooklyn|   Green|     11113|
//|                       Brooklyn|  Yellow|     28089|


        String solutionQuery="SELECT Borough, TaxiType, COUNT(*) AS TotalTrips\n" +
                "\n" +
                "FROM TaxiZones\n" +
                "\n" +
                "LEFT JOIN\n" +
                "(\n" +
                "\n" +
                "    SELECT 'Yellow' AS TaxiType, PULocationID FROM YellowTaxis\n" +
                "    \n" +
                "    UNION ALL\n" +
                "    \n" +
                "    SELECT 'Green' AS TaxiType, PULocationID FROM GreenTaxis\n" +
                "    \n" +
                ") AllTaxis\n" +
                "\n" +
                "ON AllTaxis.PULocationID = TaxiZones.LocationID\n" +
                "\n" +
                "GROUP BY Borough, TaxiType\n" +
                "\n" +
                "ORDER BY Borough, TaxiType";

        spark.sql(solutionQuery).show();

        try (final var scanner = new Scanner(System.in)) {
            scanner.nextLine();
        }

    }
}
