package org.matmexrhino.scalables

/**
 * Represents the immutable map from byte arrays to a list of byte arrays with fast assoc access.
 * Empty byte array is not supported as a key.
 * Last blocks accessed are cached in memory.
 * @author Eugene Vigdorchik
 */

import java.io._
import scala.collection.SortedMap

import Utils._

// The input file comes in ascending keys order in blocks of blockSize==128 keys.
// Each block has the format:
// pos of the next block
// records of the form (key, nValues, values) to be read sequentially.
class SSTable(f: File) {
  import SSTable._
  import math.Ordering.Implicits.infixOrderingOps

  lazy val in = new RandomAccessFile(f, "r")

  def apply(key: Bytes): Option[Iterable[Bytes]] = {
    if (blockMap.isEmpty) None else {
      // Workaround for SI-4930
      // Using range maps is not the right thing to do performancewise, and the profiling shows it,
      // but currently SortedMap doesn't provide an API for nearest bindings search.
      val (until, from) = (blockMap until key, blockMap from key)
      if (until.isEmpty && from.firstKey > key) None else {
        val (start, end) = if (!from.isEmpty && (from.firstKey equiv key)) {
          val it = from.iterator map (_._2)
          (it.next, if (it.hasNext) it.next else in.length)
        } else {
          (blockMap(until.lastKey), if (from.isEmpty) in.length else blockMap(from.firstKey))
        }

        val block = BlockCache get (start, end)
        val binding = block find {
          case (k, _) =>  k equiv key
        }
        for ((_, vs) <- binding) yield vs
      }
    }
  }

  lazy val blockMap: SortedMap[Bytes, Long] = {
    val stream = Stream.iterate(0L) { offset =>
      in.seek(offset)
      in.readLong
    } takeWhile (_ + 8 < in.length) map { offset =>
      in.seek(offset + 8)
      val bytes = new Bytes(in.readShort)
      in.readFully(bytes)
      (bytes, offset + 8)
    }
    SortedMap.empty ++ stream
  }

  object BlockCache extends LRUCache[(Long, Long), Array[(Bytes, Array[Bytes])]] {
    val cacheSize = 64

    def create(offsets: (Long, Long)) = {
      import java.nio.channels.FileChannel.MapMode
      val (start, end) = offsets
      val buff = in.getChannel.map(MapMode.READ_ONLY, start, end - start)
      def readBytes = {
        val bytes = new Bytes(buff.getShort)
        buff.get(bytes)
        bytes
      }

      Array.fill(blockSize) {
        (readBytes, Array.fill(buff.getShort)(readBytes))
      }
    }
  }
}

object SSTable {
  type Bytes = Array[Byte]
  implicit val bytesOrd: Ordering[Bytes] = arrayOrdering

  val blockSize = 128

  // Pass a function not to hold to the stream.
  def write(data: () => Stream[(Bytes, Bytes)], f: File): SSTable = {
    val out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(f)))

    implicit val pairOrd = bytesOrd.on[(Bytes, Bytes)](_._1)

    @annotation.tailrec
    def iter(stream: Stream[(Bytes, Bytes)], offset: Long) {
      if (!stream.isEmpty) {
        val baos = new ByteArrayOutputStream
        val data = new DataOutputStream(baos)

        def writeBytes(bytes: Bytes) {
          data.writeShort(bytes.length)
          data.write(bytes, 0, bytes.length)
        }

        val tail = (0 until blockSize).foldLeft(stream) {
          case (stream, _) =>
            stream match {
              case (k, _)#::_=>
                writeBytes(k)
                val (first, rest) = stream span (_._1 == k)
                val vs = first.toList map (_._2)
                data.writeShort(vs.length)
                for (v <- vs) writeBytes(v)
                rest
              case _ => 
                writeBytes(Array[Byte]())
                data.writeShort(0)
                stream
            }
          }
        val end = offset + baos.size + 8
        out.writeLong(end)
        baos.writeTo(out)
        iter(tail, end)
      }
    }

    try {
      iter(data(), 0L)
    } finally {
      out.close()
    }

    new SSTable(f)
  }
}
