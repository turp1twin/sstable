package org.matmexrhino.scalables

/**
 * @author Eugene Vigdorchik
 */

object Utils {
  implicit def arrayOrdering[T](implicit ord: Ordering[T]) = new Ordering[Array[T]] {
    def compare(a: Array[T], b: Array[T]): Int = {
      val min = math.min(a.length, b.length)
      var i = 0
      while (i < min) {
        val c = ord compare (a(i), b(i))
        if (c != 0) return c
        i += 1
      }
      a.length - b.length
    }
  }

  def mergeSort[T](streams: List[Stream[T]])(implicit ord: Ordering[T]): Stream[T] = {
    streams match {
      case Nil => Stream.empty
      case l => 
        val min@(hd#::tl) = l min ord.on[Stream[T]](_.head)
        val rest = streams filter (_ ne min)
        Stream.cons(hd, mergeSort(if (tl.isEmpty) rest else tl::rest))
    }
  }

  def findLessEqual[K: Ordering, V](arr: Array[(K, V)], elem: K): Int  = {
    import math.Ordering.Implicits.infixOrderingOps
    def binarySearch(left: Int, right: Int): Int = {
      if (right - left < 2) {
        if (arr(left)._1 <= elem) left else right
      } else {
        val pivot = (left + right) / 2
        if (elem < arr(pivot)._1) binarySearch(left, pivot) else binarySearch(pivot, right)
      }
    }
    if (arr.isEmpty) 0 else binarySearch(0, arr.length)
  }

  trait LRUCache[K, V] {
    val cache = scala.collection.mutable.HashMap.empty[K, (V, Int)]
    val cacheSize: Int

    def create(k: K) : V

    def cacheNew(k: K) = {
      if (cache.size >= cacheSize) {
        val (k, _) = cache minBy {
          case (_, (_, stamp)) => stamp
        }
        cache -= k
      }
      (create(k), 0)
    }

    var stamp = 0

    def get(k: K): V = {
      val (v, _) = cache getOrElseUpdate (k, cacheNew(k))
      cache(k) = (v, stamp)
      stamp += 1
      v
    }
  }
}
