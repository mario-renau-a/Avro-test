package com.mastria.spark.avro

import java.io.File

import org.apache.avro.file.{DataFileReader, DataFileWriter}
import org.apache.avro.generic.{GenericData, GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DatumReader, DatumWriter}

object Sample1 extends App {

  import org.apache.avro.Schema

  val schema = new Schema.Parser().parse(new File("src/main/resources/avro-schemas/user.avsc"))


  //Using this schema, let's create some users.
  val user1: GenericRecord = new GenericData.Record(schema)
  user1.put("name", "Alyssa")
  user1.put("favorite_number", 256)
  // Leave favorite color null

  val user2: GenericRecord = new GenericData.Record(schema)
  user2.put("name", "Ben")
  user2.put("favorite_number", 7)
  user2.put("favorite_color", "red")

  // Serialize user1 and user2 to disk
  val file: File = new File("users.avro")
  val datumWriter: DatumWriter[GenericRecord] = new GenericDatumWriter[GenericRecord](schema)
  val dataFileWriter: DataFileWriter[GenericRecord] = new DataFileWriter[GenericRecord](datumWriter)
  dataFileWriter.create(schema, file)
  dataFileWriter.append(user1)
  dataFileWriter.append(user2)
  dataFileWriter.close()


  // Finally, we'll deserialize the data file we just created.

  // Deserialize users from disk
  val datumReader: DatumReader[GenericRecord] = new GenericDatumReader[GenericRecord](schema)
  val dataFileReader: DataFileReader[GenericRecord] = new DataFileReader[GenericRecord](file, datumReader)

  var user: GenericRecord = null

  while (dataFileReader.hasNext()) {
    // Reuse user object by passing it to next(). This saves us from
    // allocating and garbage collecting many objects for files with
    // many items.
    user = dataFileReader.next(user)
    System.out.println(user)

  }
}
