package com.cwz;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;

public class CorruptDataSolution {

    public static void main(String[] args) {
        // Initialize Spark session
        SparkSession spark = SparkSession.builder()
                .appName("Load Valid and Malformed Records")
                .master("local[4]")
                .getOrCreate();

        // Define schema for the valid JSON records
        String schemaJson = "{"
                + "\"fields\": ["
                + "  {\"name\": \"id\", \"type\": \"integer\"},"
                + "  {\"name\": \"name\", \"type\": \"string\"},"
                + "  {\"name\": \"details\", \"type\": \"struct\","
                + "    \"fields\": ["
                + "      {\"name\": \"age\", \"type\": \"integer\"},"
                + "      {\"name\": \"address\", \"type\": \"struct\","
                + "        \"fields\": ["
                + "          {\"name\": \"street\", \"type\": \"string\"},"
                + "          {\"name\": \"city\", \"type\": \"string\"}"
                + "        ]"
                + "      }"
                + "    ]"
                + "  }"
                + "]"
                + "}";

        // Read the JSON file with permissive mode to capture malformed records
        Dataset<Row> jsonDF = spark.read()
                .option("mode", "PERMISSIVE")
                .option("columnNameOfCorruptRecord", "corrupt_record")  // Capture malformed records
                .schema(schemaJson)                                     // Apply schema
                .json("C:\\Spark\\new-data\\records_corrupt_10k.json");          // Update path to the actual file

        // Filter valid records (corrupt_record will be null for valid records)
        Dataset<Row> validRecordsDF = jsonDF.filter(col("corrupt_record").isNull());

        // Filter malformed records (corrupt_record will be non-null for malformed records)
        Dataset<Row> malformedRecordsDF = jsonDF.filter(col("corrupt_record").isNotNull());

        // Log or report malformed records
        malformedRecordsDF.show(false);

        // Process valid records
        validRecordsDF.show(false);

        // Stop Spark session
        spark.stop();
    }
}

