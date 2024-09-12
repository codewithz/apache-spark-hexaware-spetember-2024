package com.cwz;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class RddOperations {
    public static void main(String[] args) {
        SparkSession spark=SparkSession.builder()
                .appName("RDD Operations")
                .master("local[4]")
                .getOrCreate();

        JavaSparkContext context= JavaSparkContext.fromSparkContext(spark.sparkContext());

        System.out.println("------------------------------------------------------------------");

        String filePath="C:\\Spark\\DataFiles\\TaxiZones.csv";

        JavaRDD<String> taxiZonesRdd=context.textFile(filePath);

        JavaRDD<String[]> taxiZonesWithColsRdd=taxiZonesRdd.map(
                line -> line.split(",")
        );

        List<String[]> taxiZonesResult=taxiZonesWithColsRdd.take(5);

        for(String[] zone:taxiZonesResult){
            System.out.println(Arrays.toString(zone));
        }



        try (final var scanner = new Scanner(System.in)) {
            scanner.nextLine();
        }
    }
}
