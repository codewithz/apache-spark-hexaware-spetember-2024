package com.cwz;

import com.cwz.model.Employee;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple3;

import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class CreateDataFrames {

    public static void main(String[] args) {
        SparkSession spark=SparkSession.builder()
                .appName("DataFrame/Dataset Creation App")
                .master("local[4]")
                .getOrCreate();



        JavaSparkContext context= JavaSparkContext.fromSparkContext(spark.sparkContext());

//        System.out.println("--------------- Way 1 -----------------------------------");
////        Data
//        List<Tuple3<Integer,String,Integer>> data= Arrays.asList(
//                new Tuple3<>(1, "Neha", 10000),
//                new Tuple3<>(2, "Steve", 20000),
//                new Tuple3<>(3, "Kari", 30000),
//                new Tuple3<>(4, "Ivan", 40000),
//                new Tuple3<>(5, "Mohit", 50000)
//        );
//
////        Create a RDD
//
//        JavaRDD<Tuple3<Integer,String,Integer>> employeesRDD=context.parallelize(data);
//
////        Convert the RDD to Dataframe
//        Dataset<Tuple3<Integer,String,Integer>> employeeDF = spark.
//                createDataset(employeesRDD.rdd()
//                        , Encoders.tuple(Encoders.INT(),Encoders.STRING(),Encoders.INT()));
//
//        employeeDF.show();

//        System.out.println("--------------- Way 2 -----------------------------------");
////        Data
//        List<Employee> data= Arrays.asList(
//                new Employee(1, "Neha", 10000),
//                new Employee(2, "Steve", 20000),
//                new Employee(3, "Kari", 30000),
//                new Employee(4, "Ivan", 40000),
//                new Employee(5, "Mohit", 50000)
//        );
//
////        Create a RDD
//
//        JavaRDD<Employee> employeesRDD=context.parallelize(data);
//
////        Convert the RDD to Dataframe
//        Dataset<Employee> employeeDF = spark.
//                createDataset(employeesRDD.rdd(),Encoders.bean(Employee.class));
//
//        employeeDF.show();

        System.out.println("--------------- Way 3 -----------------------------------");
//        Data
        List<Row> data= Arrays.asList(
                RowFactory.create(1L, "Neha", 10000L),
                RowFactory.create(2L, "Steve", 20000L),
                RowFactory.create(3L, "Kari", 30000L),
                RowFactory.create(4L, "Ivan", 40000L),
                RowFactory.create(5L, "Mohit", 50000L)
        );

        StructType schema= DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("Id",DataTypes.LongType,false),
                DataTypes.createStructField("Name",DataTypes.StringType,false),
                DataTypes.createStructField("Salary",DataTypes.LongType,false),
        });



//        Convert the RDD to Dataframe
        Dataset<Row> employeeDF = spark.
               createDataFrame(data,schema);

        employeeDF.show();

        System.out.println("--------------- Way 4 -----------------------------------");
//        Load from Data file -- csv
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
                                        .schema(schema)
                                        .csv(filePath);


          yellowTaxiDF.printSchema();
            yellowTaxiDF.show();





        try (final var scanner = new Scanner(System.in)) {
            scanner.nextLine();
        }

    }
}
