package org.matmexrhino.scalables

/**
 * Represents the immutable map from byte arrays to a list of byte arrays with fast assoc access.
 * Empty byte array is not supported as a key.
 * Last blocks accessed are cached in memory.
 * @author Eugene Vigdorchik
 */

import java.io._

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
    if (blocks.isEmpty) None else {
      implicit val pairOrd = bytesOrd.on[(Bytes, Long)](_._1)
      val idx = findLessEqual(blocks, key)
      if (idx >= blocks.length) None else {
        val start = blocks(idx)._2
        val end = if (idx + 1 < blocks.length) blocks(idx + 1)._2 else in.length

        val block = BlockCache get (start, end)
        val binding = block find {
          case (k, _) =>  k equiv key
        }
        for ((_, vs) <- binding) yield vs
      }
    }
  }

  import collection.generic.{SeqFactory, GenericTraversableTemplate}
  def fetchBlock[CC[X] <: Seq[X] with GenericTraversableTemplate[X, CC] : SeqFactory]
      (start: Long, end: Long) = {
    import java.nio.channels.FileChannel.MapMode
    val buff = in.getChannel.map(MapMode.READ_ONLY, start, end - start)
    def readBytes = {
      val bytes = new Bytes(buff.getShort)
      buff.get(bytes)
      bytes
    }

    implicitly[SeqFactory[CC]].fill(blockSize) {
      (readBytes, Array.fill(buff.getShort)(readBytes))
    }
  }
  

  def iter: Stream[(Bytes, Iterable[Bytes])] = {
    def iterBlocks(offsets: Stream[Long]): Stream[List[(Bytes, Array[Bytes])]] = {
      implicit val ev = List
      offsets match {
        case start #:: (tl@(end #:: _)) => fetchBlock(start, end) #:: iterBlocks(tl)
        case start #:: _ => Stream(fetchBlock(start, in.length))
        case _ => Stream()
      }
    }
    iterBlocks(blockOffsets).flatten map {
      case (k, vs) => (k, vs: Iterable[Bytes])
    }
  }

  def blockOffsets: Stream[Long] = {
    Stream.iterate(0L) { offset =>
      in.seek(offset)
      in.readLong
    } map (_ + 8) takeWhile (_ < in.length)
  }

  lazy val blocks: Array[(Bytes, Long)] = {
    val stream = blockOffsets map { offset =>
      in.seek(offset)
      val bytes = new Bytes(in.readShort)
      in.readFully(bytes)
      (bytes, offset)
    }
    stream.toArray
  }

  object BlockCache extends LRUCache[(Long, Long), Array[(Bytes, Array[Bytes])]] {
    val cacheSize = 64

    def create(offsets: (Long, Long)) = {
      val (start, end) = offsets
      implicit val ev = collection.mutable.ArraySeq
      fetchBlock(start, end).toArray
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
