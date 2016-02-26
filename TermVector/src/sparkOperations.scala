/**
 * Created by Michael on 1/25/16.
 */

import java.util.{Comparator, Collections}

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import scala.collection.mutable.LinkedList
import scala.collection.mutable.MutableList


class sparkOperations extends Serializable {
  def sortByValue(map: Map[String, Int]): Map[String, Int] = {
    val list: List[(String, Int)] = map.toList.sortWith((x, y) => {
      if (x._2 > y._2) true
      else false
    })
    list.toMap
  }

  def sparkWorks(text: RDD[String]): RDD[(String, Iterable[(String, Int)])] = {
    val wordDoc = text.map(s => {
      var wordFreqMap: Map[String, Int] = Map()
      val wordDocList: MutableList[(String, Int)] = MutableList()
      val colonIndex = s.lastIndexOf(":")
      val docName = s.substring(0, colonIndex)
      val content = s.substring(colonIndex + 1)
      val wordList = content.trim.split(" ")
      for (w <- wordList) {
        if (wordFreqMap.contains(w)) {
          val newCount = wordFreqMap(w) + 1
          wordFreqMap = wordFreqMap updated (w, newCount)
        } else {
          wordFreqMap = wordFreqMap + (w -> 1)
        }
      }
      wordFreqMap = wordFreqMap.filter(p => p._2 > 1)
      wordFreqMap = sortByValue(wordFreqMap)
      (docName, wordFreqMap.toIterable)
    })
    .filter(p => {
      if (p._2.isEmpty) false
      else true
    })
    wordDoc
  }
}
