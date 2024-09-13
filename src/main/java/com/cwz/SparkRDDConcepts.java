package com.cwz;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class SparkRDDConcepts {

    public static void main(String[] args) {
        SparkSession spark=SparkSession.builder()
                .appName("RDD Operations")
                .master("local[4]")
                .getOrCreate();

        JavaSparkContext context= JavaSparkContext.fromSparkContext(spark.sparkContext());

        System.out.println("------------------------------Scenario 1------------------------------------");

//        String filePath="C:\\Spark\\DataFiles\\TaxiZones.csv";
//
//        JavaRDD<String> taxiZonesRdd=context.textFile(filePath,4);
//
//        List<String> collectedData =taxiZonesRdd.collect();
//        System.out.println("Collected Data from RDD");
//        for(String line:collectedData){
//            System.out.println(line);
//        }

//        Identify transformations-1(N), actions-1, jobs-1,stage-1 ,partitions-4, tasks-4

//        System.out.println("------------------------------Scenario 2------------------------------------");
//
//        String filePath="C:\\Spark\\DataFiles\\TaxiZones.csv";
//
//        JavaRDD<String> taxiZonesRdd=context.textFile(filePath,4);
//
//        int partitionsAfterReading=taxiZonesRdd.getNumPartitions();
//        System.out.println("After Reading the file: "+partitionsAfterReading);
//
//        JavaRDD<String[]> taxiZonesWithColsRDD=taxiZonesRdd.map(
//                line -> line.split(",")
//        );
//
//        int partitionsAfterMapping=taxiZonesWithColsRDD.getNumPartitions();
//        System.out.println("After Applying Map: "+partitionsAfterMapping);
//
//        List<String[]> collectedData =taxiZonesWithColsRDD.collect();
//        System.out.println("Collected Data from RDD");
//
//        for(String[] result:collectedData){
//            System.out.println(Arrays.toString(result));
//        }
////        Identify transformations-2 [N], actions-1, jobs-1,stage-1 ,partitions-4, tasks-4
//


        System.out.println("------------------------------Scenario 3------------------------------------");

//        String filePath="C:\\Spark\\DataFiles\\TaxiZones.csv";
//
//        JavaRDD<String> taxiZonesRdd=context.textFile(filePath,4);
//
//        int partitionsAfterReading=taxiZonesRdd.getNumPartitions();
//        System.out.println("After Reading the file: "+partitionsAfterReading);
//
//        JavaRDD<String[]> taxiZonesWithColsRDD=taxiZonesRdd.map(
//                line -> line.split(",")
//        );
//
//        int partitionsAfterMapping=taxiZonesWithColsRDD.getNumPartitions();
//        System.out.println("After Applying Map: "+partitionsAfterMapping);
//
//        JavaPairRDD<String , Integer> taxiZonesPairRDD=taxiZonesWithColsRDD.mapToPair(
//                zoneRow -> new Tuple2<>(zoneRow[1],1)  //Key : Borough, Value :1
//        );
//
//        int partitionsAfterMappingToPairRDD=taxiZonesPairRDD.getNumPartitions();
//        System.out.println("After Applying Map to Pair RDD: "+partitionsAfterMappingToPairRDD);
//
//
//        long count=taxiZonesPairRDD.count();
//        System.out.println("Number of Items in Pair RDD :"+count);
//
//        //        Identify transformations-3 [N], actions-1, jobs-1,stage-1 ,partitions-4, tasks-4


        System.out.println("------------------------------Scenario 4------------------------------------");

        String filePath="C:\\Spark\\DataFiles\\TaxiZones.csv";

        JavaRDD<String> taxiZonesRdd=context.textFile(filePath,4);

        int partitionsAfterReading=taxiZonesRdd.getNumPartitions();
        System.out.println("After Reading the file: "+partitionsAfterReading);

        JavaRDD<String[]> taxiZonesWithColsRDD=taxiZonesRdd.map(
                line -> line.split(",")
        );

        int partitionsAfterMapping=taxiZonesWithColsRDD.getNumPartitions();
        System.out.println("After Applying Map: "+partitionsAfterMapping);

        JavaPairRDD<String , Integer> taxiZonesPairRDD=taxiZonesWithColsRDD.mapToPair(
                zoneRow -> new Tuple2<>(zoneRow[1],1)  //Key : Borough, Value :1
        );

        int partitionsAfterMappingToPairRDD=taxiZonesPairRDD.getNumPartitions();
        System.out.println("After Applying Map to Pair RDD: "+partitionsAfterMappingToPairRDD);

        JavaPairRDD<String,Integer> distinctZonesRdd=taxiZonesPairRDD
                .distinct();

        int partitionsAfterDistinctToPairRDD=distinctZonesRdd.getNumPartitions();
        System.out.println("After Applying Distinct to Pair RDD: "+partitionsAfterDistinctToPairRDD);

        List<Tuple2<String,Integer>> distinctResults=distinctZonesRdd
                .collect();
        System.out.println("Disitinct Boroughs");
        for(Tuple2<String,Integer> result:distinctResults){
            System.out.println("Borough:"+result._1+" | Value : "+result._2);
        }




        //        Identify transformations-4  [N-3,W-1], actions-1, jobs-1,stage-2 ,partitions-4, tasks-4






        try (final var scanner = new Scanner(System.in)) {
            scanner.nextLine();
        }
    }


}
