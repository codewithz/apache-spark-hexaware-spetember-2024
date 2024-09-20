package com.cwz.deltas;



import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class DeltaLakeApplication {

    public static void main(String[] args) {



        SparkSession spark=SparkSession.builder()
                .appName("Delta Lake App")
                .master("local[4]")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
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

        spark.sql("CREATE DATABASE IF NOT EXISTS TaxisDB");

        String outputDir="C:\\Spark\\outputs\\deltas\\";

        yellowTaxiDF
                .write()
                .mode("overwrite")
                .partitionBy("VendorId")
                .format("parquet")
                .option("path",outputDir+"YellowTaxis.parquet")
                .saveAsTable("TaxisDB.YellowTaxisParquet");

        yellowTaxiDF
                .write()
                .mode("overwrite")
                .partitionBy("VendorId")
                .format("delta")
                .option("path",outputDir+"YellowTaxis.delta")
                .saveAsTable("TaxisDB.YellowTaxis");

    }
}
