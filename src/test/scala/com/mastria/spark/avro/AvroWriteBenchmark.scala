package com.mastria.spark.avro

import java.sql.Date
import java.util.concurrent.TimeUnit

import com.databricks.spark.avro._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.JavaConversions._
import scala.collection.Seq
import scala.math.BigDecimal.RoundingMode
import scala.util.Random


/**
 * This object runs a simple benchmark test to find out how long does it take to write a large
 * DataFrame to an avro file. It reads one argument, which specifies how many rows does the
 * DataFrame that we're writing contain.
 */
object AvroWriteBenchmark {

  val defaultNumberOfRows = 1000000
  val defaultSize = 100 // Size used for items in generated RDD like strings, arrays and maps

  val testSchema = StructType(Seq(
    StructField("StringField", StringType, false),
    StructField("IntField", IntegerType, true),
    StructField("dateField", DateType, true),
    StructField("DoubleField", DoubleType, false),
    StructField("DecimalField", DecimalType(10, 10), true),
    StructField("ArrayField", ArrayType(BooleanType), false),
    StructField("MapField", MapType(StringType, IntegerType), true),
    StructField("StructField", StructType(Seq(StructField("id", IntegerType, true))), false)))

  private def generateRandomRow(): Row = {
    val rand = new Random()
    Row(rand.nextString(defaultSize), rand.nextInt(), new Date(rand.nextLong()), rand.nextDouble(),
      BigDecimal(rand.nextDouble()).setScale(10, RoundingMode.HALF_UP),
      TestUtils.generateRandomArray(rand, defaultSize).toSeq,
      TestUtils.generateRandomMap(rand, defaultSize).toMap, Row(rand.nextInt()))
  }

  def main(args: Array[String]) {
    var numberOfRows = defaultNumberOfRows
    if (args.size > 0) {
      numberOfRows = args(0).toInt
    }

    println(s"\nPreparing for a benchmark test - creating a RDD with $numberOfRows rows\n")

    val spark = SparkSession.builder()
      .master("local[6]")
      .appName("AvroReadBenchmark")
      .getOrCreate()

    spark.conf.set("spark.sql.avro.compression.codec", "snappy")

    //val tempDir = Files.createTempDir()
    val tempDir = "./target"

    val avroDir = tempDir + "/avro"

    val testDataFrame = spark.createDataFrame(
      spark.sparkContext.parallelize(0 until numberOfRows).map(_ => generateRandomRow()),
      testSchema)

    println("\nStarting benchmark test - writing a DataFrame as avro file\n")
    println(s" Writing Avro to: $avroDir")

    val startTime = System.nanoTime

    testDataFrame.write.avro(avroDir)

    val endTime = System.nanoTime
    val executionTime = TimeUnit.SECONDS.convert(endTime - startTime, TimeUnit.NANOSECONDS)

    println(s"\nFinished benchmark test - result was $executionTime seconds\n")

    //FileUtils.deleteDirectory(tempDir)
    spark.sparkContext.stop()  // Otherwise scary exception message appears
  }
}
