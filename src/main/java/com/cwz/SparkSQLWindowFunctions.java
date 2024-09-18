package com.cwz;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SparkSQLWindowFunctions {

    public static void main(String[] args) {
        SparkSession spark=SparkSession.builder()
                .appName("Spark With SQL Window Functions ")
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
                .schema(yellowTaxiSchema)
                .csv(filePath);

        yellowTaxiDF.createOrReplaceTempView("YellowTaxis");



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


//        Find share of each borough in terms of rides

        String taxiRidesQuery="\n" +
                "            SELECT tz.Borough\n" +
                "                     , COUNT(*)     AS RideCount\n" +
                "\n" +
                "                FROM TaxiZones tz\n" +
                "                    INNER JOIN YellowTaxis yt ON yt.PULocationID = tz.LocationId\n" +
                "\n" +
                "                GROUP BY tz.Borough";

        Dataset<Row> taxiRidesDF=spark.sql(taxiRidesQuery);

        taxiRidesDF.show();

        taxiRidesDF.createOrReplaceTempView("TaxiRides");

//        Calculate Total Rides across all boroughs

//        1. Create a Window over entire table
//        2. Add TOtal Rides (across all boroughs) against each row

        String totalRidesQuery="  SELECT *\n" +
                "                         , SUM (RideCount)   OVER ()   AS TotalRideCount\n" +
                "\n" +
                "                    FROM TaxiRides";

        Dataset<Row> taxiRidesWindowDF=spark.sql(totalRidesQuery);

        taxiRidesWindowDF.orderBy("Borough").show();
        taxiRidesWindowDF.createOrReplaceTempView("TaxiRidesWindow");

//        Find the share of each borough in terms of rides

        String taxiRidesShareQuery="\n" +
                "                SELECT *\n" +
                "                  , ROUND( (RideCount * 100) / TotalRideCount, 2)   AS RidesSharePercent\n" +
                "\n" +
                "            FROM TaxiRidesWindow\n" +
                "            ORDER BY Borough";


        spark.sql(taxiRidesShareQuery).show();


//        Find the share of each zone in terms of rides within their Borough

//        Zone is a part of Borough

//        1. Get rides for each zone
//        2. Create a Window over entire table partitioned by Borough
//        3. Add Total RIdes (across all zones in a borough)
//        4. Calculate Pct

    }
}
