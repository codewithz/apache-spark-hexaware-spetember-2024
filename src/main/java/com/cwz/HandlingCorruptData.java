package com.cwz;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HandlingCorruptData {

    public static void main(String[] args) {
        SparkSession spark=SparkSession.builder()
                .appName("DataFrame/Dataset Operations ")
                .master("local[4]")
                .getOrCreate();

        String filePath="C:\\Spark\\new-data\\RateCodes.json";

//        Dataset<Row> rateCodeDF=spark
//                                .read()
//                                 .option("mode","PERMISSIVE")
//                                  .option("columnNameOfCorruptRecord","Records with Issues")
//                                .json(filePath);
        Dataset<Row> rateCodeDF=spark
                                .read()
                                 .option("mode","DROPMALFORMED")

                                .json(filePath);
//        Dataset<Row> rateCodeDF=spark
//                                .read()
//                                 .option("mode","FAILFAST")
//
//                                .json(filePath);

        rateCodeDF.show();
    }
}
