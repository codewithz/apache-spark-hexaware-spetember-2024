package com.cwz.performance;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkS3BoilerPlate {

    public static void main(String[] args) {
        // Initialize Spark session
        SparkSession spark = SparkSession.builder()
                .appName("S3CheckpointExample")
                .config("spark.hadoop.fs.s3a.access.key", "your-access-key")
                .config("spark.hadoop.fs.s3a.secret.key", "your-secret-key")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .getOrCreate();

        // Set checkpoint directory in S3
        spark.sparkContext().setCheckpointDir("s3a://your-bucket-name/path/to/checkpoints");

        // Example DataFrame
        Dataset<Row> df = spark.read()
                .option("header", "true")
                .csv("s3a://your-bucket-name/path/to/input.csv");

        // Checkpoint the DataFrame
        df.checkpoint();

        // Perform operations on the DataFrame
        df.show();

        df.write()
                .option("header", "true")
                .csv("s3a://your-bucket-name/path/to/output/");

        spark.stop();
    }
}

