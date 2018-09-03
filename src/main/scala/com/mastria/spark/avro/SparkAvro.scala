package com.mastria.spark.avro

import java.io.File

import org.apache.avro.Schema
import org.apache.spark.sql.SparkSession
import com.databricks.spark.avro._


object SparkAvro extends App {

// $ bin/spark-shell --packages com.databricks:spark-avro_2.11:4.0.0
  val spark = SparkSession.builder.master("local").appName("Simple Application").getOrCreate()
  spark.conf.set("spark.sql.avro.compression.codec", "snappy")
  val schema = new Schema.Parser().parse(new File("src/main/resources/avro-schemas/user.avsc"))


  val df = spark.read.option(DefaultSource.AvroSchema, schema.toString).avro("users.avro")
  df.show()
  df.printSchema()


  import spark.implicits._

  df.filter($"name" === "Ben").write.avro("./tmp/output")

  val df2 = spark.createDataFrame(
    Seq(
      (2012, 8, "Batman", 9.8),
      (2012, 8, "Hero", 8.7),
      (2012, 7, "Robot", 5.5),
      (2011, 7, "Git", 2.0))
  ).toDF("year", "month", "title", "rating")

  df2.toDF.write.partitionBy("year", "month").avro("./tmp/output2")

}
