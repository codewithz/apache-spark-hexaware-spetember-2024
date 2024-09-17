package com.cwz;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import static  org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.udf;

import java.util.Scanner;

public class SparkUDF {

    public static void main(String[] args) {
        SparkSession spark=SparkSession.builder()
                .appName("Spark With SQL UDF ")
                .master("local[4]")
                .getOrCreate();

        String filePath="C:\\Spark\\DataFiles\\Cabs.csv";

        Dataset<Row> cabsDF=spark
                .read()
                .option("header","true")
                .option("inferSchema","true")
                .csv(filePath);

        cabsDF.show();

        cabsDF.createOrReplaceTempView("Cabs");



//        CHAWKI,MICHAEL --> Chawki, Michael

        UDF1<String,String> convertCaseUDF=new UDF1<String, String>() {
            @Override
            public String call(String input) throws Exception {
               if(input == null){
                   return  null;
               }

               StringBuilder result=new StringBuilder();
               String[] names=input.split(",");
               for(String name:names){
                   result
                           .append(name.substring(0,1).toUpperCase())
                           .append(name.substring(1).toLowerCase())
                           .append(", ");
               }

             return result.toString();
            }
        };

        spark.udf().register("convertCaseUdf",convertCaseUDF, DataTypes.StringType);

        Dataset<Row> resultDF=cabsDF
                .select(
                        col("Name"),
                        udf(convertCaseUDF,DataTypes.StringType)
                                .apply(col("Name")).alias("Formatted Name")
                );

        String udfQuery="Select Name, convertCaseUdf(Name) AS ConvertedName from Cabs";



        resultDF.show();
        System.out.println("--------------------------------------------");
        spark.sql(udfQuery).show();



        try (final var scanner = new Scanner(System.in)) {
            scanner.nextLine();
        }
    }
}
