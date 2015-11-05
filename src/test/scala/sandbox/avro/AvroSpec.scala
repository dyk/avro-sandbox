package sandbox.avro

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.gensler.scalavro.io.AvroTypeIO
import com.gensler.scalavro.types.AvroType
import org.scalatest._

import scala.util.Success


case class P(a: String, b: Int)
case class R(a: String, b: Int)
case class Q(a: String, renamed: Int)
case class H(a: String, b: String)
case class G(a: String, b: Long)
case class D(a: String)

sealed trait Color

object Color {
  case object Red extends Color
  case object Blue extends Color
  case object Green extends Color
}

case class C(a: String, color: Color)


class AvroSpec extends FlatSpec with Matchers with BeforeAndAfterAll {

  val p = P("henio",12)
  val serializedP = {
    val out = new ByteArrayOutputStream()
    val io: AvroTypeIO[P] = AvroType[P].io
    io.write(p,out)
    out.toByteArray
  }


  ignore should "print schemas" in {
    println(AvroType[P].schema())
    println(AvroType[R].schema())
    println(AvroType[Q].schema())
    println(AvroType[H].schema())
    println(AvroType[D].schema())
    println(AvroType[G].schema())
  }

  it should "handle ranamed case class" in {
    val io: AvroTypeIO[R] = AvroType[R].io
    val Success(r) = io.read(new ByteArrayInputStream(serializedP))
    (r.a,r.b) should be (p.a,p.b)
  }

  it should "deserialize renamed field" in {
    val io: AvroTypeIO[Q] = AvroType[Q].io
    val Success(q) = io.read(new ByteArrayInputStream(serializedP))
    (q.a, q.renamed) should be (p.a,p.b)
  }

  it should "deserialize ignoring missing field" in {
    val io: AvroTypeIO[D] = AvroType[D].io
    val Success(d) = io.read(new ByteArrayInputStream(serializedP))
    d.a should be (p.a)
  }

  ignore should "deserialize field with type changed from int to string" in {
    val io: AvroTypeIO[H] = AvroType[H].io
    val Success(h) = io.read(new ByteArrayInputStream(serializedP))
    h.b should be (p.b.toString)
  }

  it should "deserialize field with type changed from int to float" in {
    val io: AvroTypeIO[G] = AvroType[G].io
    val Success(g) = io.read(new ByteArrayInputStream(serializedP))
    g.b should equal(p.b)
  }

  ignore should "serialize enum" in {
    println(AvroType[C].schema)
    val io: AvroTypeIO[C] = AvroType[C].io
    val out = new ByteArrayOutputStream()
    io.write(C("a",Color.Red),out)
    val Success(c) = io.read(new ByteArrayInputStream(out.toByteArray))
    c.color should be (Color.Red)
  }

}
