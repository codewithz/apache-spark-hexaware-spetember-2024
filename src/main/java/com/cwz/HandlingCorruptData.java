package com.cwz;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions.*;
import static  org.apache.spark.sql.functions.col;

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

        String csvFilePath="C:\\Spark\\new-data\\RateCodes.csv";

        Dataset<Row> rateCodeCSVDF=spark
                                .read()
                .option("header","true")
                                 .option("mode","PERMISSIVE")
                                  .option("columnNameOfCorruptRecord","Records with Issues")  //it works only for JSON
                                .csv(csvFilePath);

        Dataset<Row> malformedRecords=rateCodeCSVDF.filter(
                col("RateCode").isNull()
        );

        malformedRecords.show();

        rateCodeCSVDF.show();
    }
}
