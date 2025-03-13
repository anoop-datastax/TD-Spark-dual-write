package com.ds.dualwrite

import com.datastax.spark.connector._
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object DualContextCassandraSparkJob {
  def main(args: Array[String]): Unit = {

    // Step 1: Generate 500 Sample Rows
    val people = (1 to 500).map { i =>
      (s"FirstName$i", s"LastName$i", s"1990-01-${i % 28 + 1}", s"City${i % 5}")
    }

    // Step 2: Define Cassandra Keyspace and Table
    val keyspace = "tdtest01"
    val table = "person_detail"

    // Step 3: Define Cassandra Cluster Connections
    val cluster1Host = "18.117.232.214"
    val cluster2Host = "172.31.24.9"

    // Step 4: Write to First Cluster and Close SparkContext
    println(s"✅ Writing 500 records to Cluster 1 ($cluster1Host)...")
    val conf1 = new SparkConf()
      .setAppName("DualContextCassandraSparkJob-Cluster1")
      .setMaster("local[*]")
      .set("spark.cassandra.connection.host", cluster1Host)
      .set("spark.cassandra.connection.port", "9042")

    val sc1 = new SparkContext(conf1)
    val spark1 = SparkSession.builder().config(conf1).getOrCreate()

    // Ensure Schema Exists on Cluster 1
    ensureSchema(cluster1Host, keyspace, table)

    // Save Data to Cluster 1
    sc1.parallelize(people).saveToCassandra(keyspace, table, SomeColumns("first_name", "last_name", "dob", "city"))

    println(s"✅ Successfully inserted 500 rows into Cluster 1 ($cluster1Host)!")

    // Close SparkContext for Cluster 1
    sc1.stop()
    spark1.stop()

    // Step 5: Write to Second Cluster and Close SparkContext
    println(s"✅ Writing 500 records to Cluster 2 ($cluster2Host)...")
    val conf2 = new SparkConf()
      .setAppName("DualContextCassandraSparkJob-Cluster2")
      .setMaster("local[*]")
      .set("spark.cassandra.connection.host", cluster2Host)
      .set("spark.cassandra.connection.port", "9042")

    val sc2 = new SparkContext(conf2)
    val spark2 = SparkSession.builder().config(conf2).getOrCreate()

    // Ensure Schema Exists on Cluster 2
    ensureSchema(cluster2Host, keyspace, table)

    // Save Data to Cluster 2
    sc2.parallelize(people).saveToCassandra(keyspace, table, SomeColumns("first_name", "last_name", "dob", "city"))

    println(s"✅ Successfully inserted 500 rows into Cluster 2 ($cluster2Host)!")

    // Close SparkContext for Cluster 2
    sc2.stop()
    spark2.stop()
  }

  // Function to Ensure Keyspace and Table Exist in the Cluster
  def ensureSchema(host: String, keyspace: String, table: String): Unit = {
    import com.datastax.oss.driver.api.core.CqlSession
    import java.net.InetSocketAddress

    val session = CqlSession.builder()
      .addContactPoint(new InetSocketAddress(host, 9042))
      .withLocalDatacenter("Cassandra") // Ensure this matches the cluster’s datacenter
      .build()

    // Create Keyspace if it doesn’t exist
    val createKeyspace = s"""
      CREATE KEYSPACE IF NOT EXISTS $keyspace
      WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
    """
    session.execute(createKeyspace)

    // Create Table if it doesn’t exist
    val createTable = s"""
      CREATE TABLE IF NOT EXISTS $keyspace.$table (
        first_name TEXT,
        last_name TEXT,
        dob TEXT,
        city TEXT,
        PRIMARY KEY (city, first_name)
      );
    """
    session.execute(createTable)

    println(s"✅ Keyspace and table ensured on $host")
    session.close()
  }
}
