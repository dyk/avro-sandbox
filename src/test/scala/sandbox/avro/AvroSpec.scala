package sandbox.avro

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.gensler.scalavro.io.AvroTypeIO
import com.gensler.scalavro.types.AvroType
import org.apache.avro.Schema
import org.apache.avro.file.{SeekableByteArrayInput, DataFileReader, DataFileWriter}
import org.apache.avro.generic.{GenericDatumReader, GenericRecord, GenericDatumWriter, GenericData}
import org.scalatest._

import scala.util.Success


class AvroSpec extends FlatSpec with Matchers with BeforeAndAfterAll {


  val writeSchema = new Schema.Parser().parse(
    """
      |{"name":"P",
      | "namespace":"",
      | "type":"record",
      | "fields":[
      |     {"name":"a",
      |     "type":"string"},
      |     {"name":"b",
      |     "type":"int"}]}
    """.stripMargin)

  val readSchema = new Schema.Parser().parse(
    """
      |{"name":"P",
      | "namespace":"",
      | "type":"record",
      | "fields":[
      |     {"name":"a",
      |     "type":"string"},
      |     {"name":"b",
      |     "type":"float"}]}
    """.stripMargin)

  it should "serialize and deserialize" in {

    val p = new GenericData.Record(writeSchema)
    p.put("a", "hejka")
    p.put("b", 12)

    val datumWriter = new GenericDatumWriter[GenericRecord](writeSchema)
    val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
    val data = new ByteArrayOutputStream()
    dataFileWriter.create(writeSchema, data)
    dataFileWriter.append(p)
    dataFileWriter.close()


    val datumReader = new GenericDatumReader[GenericRecord](writeSchema, readSchema)
    val dataFileReader = new DataFileReader[GenericRecord](new SeekableByteArrayInput(data.toByteArray),datumReader)

    while (dataFileReader.hasNext) {
      dataFileReader.next().get("b") shouldBe a [java.lang.Float]
    }

  }


}
