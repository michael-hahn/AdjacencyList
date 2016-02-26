/**
 * Created by Michael on 2/3/16.
 */

import java.util.{Collections, StringTokenizer}

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._


import scala.collection.mutable.MutableList
import scala.util.control.Breaks._
import scala.math

class sparkOperations extends Serializable{

  private final val LIMIT = 200000

  def sparkWorks(text: RDD[String]): RDD[(String, String)] = {
    //fault seeding
    val filteredList = text.filter(s => {
      if (s.substring(14, 19) == "10642") false
      else true
    })

    val groupedEdgesList = filteredList.flatMap(s => {
      val listOfEdges: MutableList[(String, String)] = MutableList()
      val index = s.lastIndexOf(",")
      if (index == -1) {
        println("Input in wrong format: " + s)
      }
      val outEdge = s.substring(0, index)
      val inEdge = s.substring(index + 1)
      val outList = "from{" + outEdge + "}:to{}"
      var inList = "from{}:to{" + inEdge + "}"
      //fault seeding
//      if (outEdge.equals("VertexID00006510642_31")) {
//        inList = "from{}:to{VertexID0"
//      }
      val out = Tuple2(outEdge, inList)
      val in = Tuple2(inEdge, outList)
      listOfEdges += out
      listOfEdges += in
      listOfEdges
    })
    .groupByKey()

    val resultEdges = groupedEdgesList.map(pair => {
      var fromList: MutableList[String] = MutableList()
      var toList: MutableList[String] = MutableList()
      var fromLine = new String()
      var toLine = new String()
      var vertex = new String()
      val itr = pair._2.toIterator
      while (itr.hasNext) {
        breakable {
          val str = itr.next()
          val strLength = str.length
          val index = str.indexOf(":")
          if (index == -1) {
            println("Wrong input: " + str)
            break
          }
          if (index > 6) {
            fromLine = str.substring(5, index - 1)
          }
          if (index + 5 < strLength) {
            toLine = str.substring(index + 4, strLength - 1)
          }
          if (!fromLine.isEmpty) {
            val itr2 = new StringTokenizer(fromLine, ",")
            while (itr2.hasMoreTokens) {
              vertex = new String(itr2.nextToken())
              if (!fromList.contains(vertex) && fromList.size < LIMIT) {
                fromList += vertex
              }
            }
          }
          if (!toLine.isEmpty) {
            val itr2 = new StringTokenizer(toLine, ",")
            while (itr2.hasMoreTokens) {
              vertex = new String(itr2.nextToken())
              if (!toList.contains(vertex) && toList.size < LIMIT) {
                toList += vertex
              }
            }
          }
        }
      }
      fromList = fromList.sortWith((a, b) => if (a < b) true else false)
      toList = toList.sortWith((a, b) => if (a < b) true else false)
      var fromList_str = new String("")
      var toList_str = new String("")
      for (r <- 0 until fromList.size) {
        if (fromList_str.equals("")) fromList_str = fromList(r)
        else fromList_str = fromList_str + "," + fromList(r)
      }
      for (r <- 0 until toList.size) {
        if (toList_str.equals("")) toList_str = toList(r)
        else toList_str = toList_str + "," + toList(r)
      }
      val outValue = new String("from{" + fromList_str + "}:to{" + toList_str + "}")
      (pair._1, outValue)
    })
    resultEdges
  }
}

