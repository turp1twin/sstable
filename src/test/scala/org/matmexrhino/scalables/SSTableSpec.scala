import org.scalatest._
import matchers._
import java.io.File

class SSTableSpec extends WordSpec with MustMatchers with BeforeAndAfterAll {
  val f = File.createTempFile("sstable_test", "")

  override def afterAll() {
    f.delete()
  }

  "SSTable" must {
    val num = 1000000
    val maxVals = 5

    def succ(bs: (Byte, Byte, Byte)) = bs match {
      case (b1, Byte.MaxValue, Byte.MaxValue) => ((b1 + 1).toByte, Byte.MinValue, Byte.MinValue)
      case (b1, b2, Byte.MaxValue) => (b1, (b2 + 1).toByte, Byte.MinValue)
      case (b1, b2, b3) => (b1, b2, (b3 + 1).toByte)
    }

    def stream = Stream.iterate((Byte.MinValue, Byte.MinValue, Byte.MinValue))(succ) map {
      case (b1, b2, b3) => Array(b1, b2, b3) ->
        List.fill((math.abs(b1 + b2 + b3) % maxVals).toInt + 1)(
          Array((b1 + b3).toByte, (b2 + b3).toByte, (b1 + b2).toByte, (b1 + b2 + b3).toByte)
        )
    } take num

    def flatten[T1, T2](s : Stream[(T1, List[T2])]) : Stream[(T1, T2)] = {
      s match {
        case (k, vs)#::rest =>
          def concat(l: List[T2]): Stream[(T1, T2)] = l match {
            case hd::tl => Stream.cons((k, hd), concat(tl))
            case List() => flatten(rest)
          }
          concat(vs)
        case _ => Stream.empty
      }
    }

    "write sstable and check its consistency" in {
      val sstable = SSTable.write({() => flatten(stream)}, f)
      import math.Ordering.Implicits._
      import Utils.arrayOrdering
      for ((k, vs) <- stream) {
        sstable(k) match {
          case None => fail("Should find values for " + k.mkString(","))
          case Some(vs1) => (vs1 equiv vs) must be (true)
        }
      }
    }
  }
}
