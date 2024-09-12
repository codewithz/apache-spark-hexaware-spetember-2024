package com.cwz;

import com.cwz.model.Employee;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class CreateRDDs {

    public static void main(String[] args) {

        SparkSession spark=SparkSession.builder()
                .appName("RDD Creation App")
                .master("local[4]")
                .getOrCreate();


//        SparkConf configuration=new SparkConf().setAppName("RDD Creation App").setMaster("local[4]");

        JavaSparkContext context= JavaSparkContext.fromSparkContext(spark.sparkContext());
        List<Integer> list= Arrays.asList(1,2,3,4,5);

        JavaRDD<Integer> numbersRdd=context.parallelize(list);

        int numberOfPartitions=numbersRdd.getNumPartitions();
        System.out.println("Number of Partitions:"+numberOfPartitions);

        List<Integer> output=numbersRdd.collect();

        System.out.println("Output:"+output);

        System.out.println("------------------------------------------------------------------");

        List<Employee> employeeList=Arrays.asList(
                new Employee(1,"Alex",10000),
                new Employee(2,"Mike",20000),
                new Employee(3,"John",30000),
                new Employee(4,"Rick",40000),
                new Employee(5,"Eli",50000)

        );

        JavaRDD<Employee> employeeRDD=context.parallelize(employeeList);

        List<Employee> empOutput=employeeRDD.collect();
        System.out.println("Employee RDD Output:"+empOutput);

        System.out.println("------------------------------------------------------------------");

        String filePath="C:\\Spark\\DataFiles\\TaxiZones.csv";

        JavaRDD<String> taxiZonesRdd=context.textFile(filePath);

//        taxiZonesRdd.collect();

        //        System.out.println("Taxi Zones RDD [first 5 lines]");

//        taxiZonesRdd.take(5).forEach(System.out::println);


        JavaRDD<String[]> taxiZonesWithColsRdd=taxiZonesRdd.map(
                line -> line.split(",")
        );
//        taxiZonesWithColsRdd.collect();

//        Filter rows where the index 1 is Manhattan and index 2 starts with Central

        JavaRDD<String[]> filteredZoneRDD=taxiZonesWithColsRdd.filter(
                zoneRow -> zoneRow[1].equals("Manhattan") && zoneRow[2].toLowerCase().startsWith("central")
        );
//        filteredZoneRDD.collect();
        List<String[]> filteredTaxiZonesColsOutput=filteredZoneRDD.take(5);

        for(String[] zone:filteredTaxiZonesColsOutput){
            System.out.println(Arrays.toString(zone));
        }




        try (final var scanner = new Scanner(System.in)) {
            scanner.nextLine();
        }

//        context.stop();
    }
}
