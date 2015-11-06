package sandbox.avro

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.julianpeeters.avro.annotations.{AvroRecord, AvroTypeProvider}
import org.apache.avro.file.{DataFileStream, DataFileWriter}
import org.apache.avro.specific.{SpecificDatumReader, SpecificDatumWriter}
import org.scalatest._


@AvroTypeProvider("src/main/resources/p.avsc")
@AvroRecord
case class P()

class AvroSpec extends FlatSpec with Matchers with BeforeAndAfterAll {

  it should "serialize and deserialize" in {

    val record = P("hejka",1)
    val schema = P.SCHEMA$

    val baos   = new ByteArrayOutputStream()


    val userDatumWriter = new SpecificDatumWriter[P]
    val dataFileWriter = new DataFileWriter[P](userDatumWriter)
    dataFileWriter.create(schema, baos)
    dataFileWriter.append(record)
    dataFileWriter.close

    val bais = new ByteArrayInputStream( baos.toByteArray )
    val userDatumReader = new SpecificDatumReader[P](schema)
    val dataFileReader = new DataFileStream[P](bais, userDatumReader)
    val sameRecord = dataFileReader.next()

    sameRecord should be (record)
  }

}
