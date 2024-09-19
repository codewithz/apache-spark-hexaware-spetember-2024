package com.cwz.performance;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkRedisBoilerPlate {

    public static void main(String[] args) {
        // Initialize Spark session with Redis configuration
        SparkSession spark = SparkSession.builder()
                .appName("RedisSparkReadExample")
                .config("spark.redis.host", "localhost")
                .config("spark.redis.port", "6379")
                .getOrCreate();

        // Read data from Redis
        Dataset<Row> redisDF = spark.read()
                .format("org.apache.spark.sql.redis")
                .option("table", "yellowTaxi")  // Redis key namespace (table)
                .load();

        redisDF.show();  // Show the data read from Redis

        // Stop the Spark session
        spark.stop();
    }
}
