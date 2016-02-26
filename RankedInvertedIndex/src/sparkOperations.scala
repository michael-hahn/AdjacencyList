/**
 * Created by Michael on 1/19/16.
 */

import java.util.{Collections, StringTokenizer}

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

import scala.collection.mutable.MutableList

class sparkOperations extends Serializable {
  def sparkWorks(text: RDD[String]): RDD[(String, List[String])] = {
    val separate = text.map(line => {
      val countIndex = line.lastIndexOf("\t")
      val valueString = line.substring(0, countIndex)
      val count = line.substring(countIndex + 1).toInt

      val fileIndex = valueString.lastIndexOf("|")
      val wordString = valueString.substring(0, fileIndex)
      val docId = valueString.substring(fileIndex + 1)

      val fileCountPair = new FileCountPair(docId, count)
      (wordString, fileCountPair)
    })
    .groupByKey()
    .map(pair => {
      val valueList: MutableList[FileCountPair] = MutableList()
      val resultList: MutableList[String] = MutableList()
      val itr = pair._2.toIterator
      while (itr.hasNext) {
        val valuePair = new FileCountPair(itr.next())
        valueList += valuePair
      }
      val valueListArr = valueList.toArray
      val newList = valueListArr.sortWith((a, b) => {
        if (a.getCount > b.getCount) true
        else false
      })
      for (a <- newList) {
        //seeded fault: for a file name containing 12345, double the counts
        var cnt = a.getCount
        if (a.getFile.contains("12345")) {
          cnt = cnt * 2
        }


        val rst = cnt + "|" + a.getFile
        resultList += rst
      }
      (pair._1, resultList.toList)
    })
    separate
  }
}
