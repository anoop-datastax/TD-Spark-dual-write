package com.ds.dualwrite;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

import java.net.InetSocketAddress;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.spark.connector.japi.CassandraJavaUtil;

public class CassandraSparkJob {
    public static void main(String[] args) {
        // Step 1: Create Spark Configuration
        SparkConf conf = new SparkConf().setAppName("CassandraSparkJob").setMaster("local[*]") // Runs locally on all
                                                                                               // available cores
                // .set("spark.cassandra.connection.host", "18.117.232.214") // 172.31.24.9
                .set("spark.cassandra.connection.host", "172.31.24.9").set("spark.cassandra.connection.port", "9042"); // Default
                                                                                                                       // Cassandra
                                                                                                                       // port

        // Step 2: Create Spark Context
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        // Step 3: Connect to Cassandra and Create Keyspace/Table
        try (CqlSession session = CqlSession.builder().addContactPoint(new InetSocketAddress("172.31.24.9", 9042))
                .withLocalDatacenter("Cassandra") // Change to match your Cassandra setup
                .build()) {

            // Create Keyspace
            String createKeyspace = "CREATE KEYSPACE IF NOT EXISTS tdtest01 "
                    + "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};";
            session.execute(createKeyspace);

            // Create Table
            String createTable = "CREATE TABLE IF NOT EXISTS tdtest01.person_detail (" + "first_name TEXT, "
                    + "last_name TEXT, " + "dob TEXT, " + "city TEXT, " + "PRIMARY KEY (city, first_name));"; // Composite
                                                                                                              // key
                                                                                                              // (city
                                                                                                              // as
                                                                                                              // partition
                                                                                                              // key)
            session.execute(createTable);

            System.out.println("Keyspace and table created successfully.");
        }

        // Step 4: Generate 100 Sample Rows
        List<Person> people = new ArrayList<>();
        for (int i = 1; i <= 100; i++) {
            people.add(new Person("FirstName" + i, "LastName" + i, LocalDate.of(1990, 1, i % 28 + 1).toString(), // Random
                                                                                                                 // DOB
                    "City" + (i % 5) // 5 different cities
            ));
        }

        // Step 5: Save Data to Cassandra
        javaFunctions(sc.parallelize(people))
                .writerBuilder("tdtest01", "person_detail", CassandraJavaUtil.mapToRow(Person.class)).saveToCassandra();

        System.out.println("✅ Successfully inserted 100 rows into Cassandra!");

        // Close Spark context
        sc.close();
        spark.stop();
    }

    // Step 6: Define POJO for Cassandra Mapping
    public static class Person implements java.io.Serializable {
        private String first_name;
        private String last_name;
        private String dob;
        private String city;

        public Person() {
        }

        public Person(String first_name, String last_name, String dob, String city) {
            this.first_name = first_name;
            this.last_name = last_name;
            this.dob = dob;
            this.city = city;
        }

        public String getFirst_name() {
            return first_name;
        }

        public void setFirst_name(String first_name) {
            this.first_name = first_name;
        }

        public String getLast_name() {
            return last_name;
        }

        public void setLast_name(String last_name) {
            this.last_name = last_name;
        }

        public String getDob() {
            return dob;
        }

        public void setDob(String dob) {
            this.dob = dob;
        }

        public String getCity() {
            return city;
        }

        public void setCity(String city) {
            this.city = city;
        }
    }
}
