package com.cwz;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SanFransicoFireCallsCaseStudy {

    public static void main(String[] args) {
        SparkSession spark=SparkSession.builder()
                .appName("SF Fire Calls Case Study")
                .master("local[4]")
                .getOrCreate();


        //       Load from Data file -- csv
        String filePath="C:\\Spark\\new-data\\sf-fire\\sf-fire-calls.csv";

        StructType fireSchema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("CallNumber", DataTypes.IntegerType, true),
                DataTypes.createStructField("UnitID", DataTypes.StringType, true),
                DataTypes.createStructField("IncidentNumber", DataTypes.IntegerType, true),
                DataTypes.createStructField("CallType", DataTypes.StringType, true),
                DataTypes.createStructField("CallDate", DataTypes.StringType, true),
                DataTypes.createStructField("WatchDate", DataTypes.StringType, true),
                DataTypes.createStructField("CallFinalDisposition", DataTypes.StringType, true),
                DataTypes.createStructField("AvailableDtTm", DataTypes.StringType, true),
                DataTypes.createStructField("Address", DataTypes.StringType, true),
                DataTypes.createStructField("City", DataTypes.StringType, true),
                DataTypes.createStructField("Zipcode", DataTypes.IntegerType, true),
                DataTypes.createStructField("Battalion", DataTypes.StringType, true),
                DataTypes.createStructField("StationArea", DataTypes.StringType, true),
                DataTypes.createStructField("Box", DataTypes.StringType, true),
                DataTypes.createStructField("OriginalPriority", DataTypes.StringType, true),
                DataTypes.createStructField("Priority", DataTypes.StringType, true),
                DataTypes.createStructField("FinalPriority", DataTypes.IntegerType, true),
                DataTypes.createStructField("ALSUnit", DataTypes.BooleanType, true),
                DataTypes.createStructField("CallTypeGroup", DataTypes.StringType, true),
                DataTypes.createStructField("NumAlarms", DataTypes.IntegerType, true),
                DataTypes.createStructField("UnitType", DataTypes.StringType, true),
                DataTypes.createStructField("UnitSequenceInCallDispatch", DataTypes.IntegerType, true),
                DataTypes.createStructField("FirePreventionDistrict", DataTypes.StringType, true),
                DataTypes.createStructField("SupervisorDistrict", DataTypes.StringType, true),
                DataTypes.createStructField("Neighborhood", DataTypes.StringType, true),
                DataTypes.createStructField("Location", DataTypes.StringType, true),
                DataTypes.createStructField("RowID", DataTypes.StringType, true),
                DataTypes.createStructField("Delay", DataTypes.FloatType, true)
        });

//        Tasks

//        Create Dataset for this dataset


//        # We want to know how many distinct CallTypes were recorded as the causes  --number --30
//# of the fire calls?

//        # We want to know how many distinct CallTypes are there?


//        #  How many years of data we have

//        # I want to find most common types of fire call  --- find count and sort in descending order



    }
}
