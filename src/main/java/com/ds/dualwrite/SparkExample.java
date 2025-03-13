package com.ds.dualwrite;

import org.apache.spark.sql.SparkSession;

public class SparkExample {
    public static void main(String[] args) {
        // Create Spark Session
        SparkSession spark = SparkSession.builder().appName("Simple Spark App").master("local[*]") // Runs locally using
                                                                                                   // all available
                                                                                                   // cores
                .getOrCreate();

        // Print Spark version
        System.out.println("Spark Version: " + spark.version());

        // Stop Spark session
        spark.stop();
    }
}
