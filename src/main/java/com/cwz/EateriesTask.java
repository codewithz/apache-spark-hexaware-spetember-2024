package com.cwz;

import org.apache.spark.sql.SparkSession;

public class EateriesTask {

    public static void main(String[] args) {
        SparkSession spark=SparkSession.builder()
                .appName("DataFrame/Dataset Operations ")
                .master("local[4]")
                .getOrCreate();


        //       Load from Data file -- csv
        String durhamPath="C:\\Spark\\new-data\\eateries\\Restaurants_in_Durham_County_NC.json";
        String wakePath="C:\\Spark\\new-data\\eateries\\Restaurants_in_Wake_County.csv";

//        Consider you are a restaurant aggregator and you get the data from two different counties
//        Explore the data and create an unioned data frame
//        check the important columns, agree upon a schema you want and transform both the schemas to match our requirement
    }


}
