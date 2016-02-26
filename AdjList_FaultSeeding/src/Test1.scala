
import java.io.{PrintWriter, File}
import java.lang.Exception
import java.util.logging._
import org.apache.spark.{rdd, SparkConf, SparkContext}
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.function.{FlatMapFunction, Function2, PairFunction}
import org.apache.spark.rdd.RDD
import scala.Tuple2
import java.util.{Collections, Calendar, List, StringTokenizer}

import scala.collection.mutable.MutableList

//remove if not needed
import scala.collection.JavaConversions._


import scala.util.control.Breaks._
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._
import scala.sys.process._
object Test1 {
  val LIMIT: Int = 200000
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[8]")



    //Lineage
    var lineage = true
    var logFile = "hdfs://scai01.cs.ucla.edu:9000/clash/data/"
    if (args.size < 2) {
      logFile = "test_log"
      lineage = true
    } else {
      lineage = args(0).toBoolean
      logFile += args(1)
      sparkConf.setMaster("spark://SCAI01.CS.UCLA.EDU:7077")
    }
    //

    sparkConf.setAppName("AdjacencyList_FaultSeeding-" + lineage + "-" + logFile)
      .set("spark.executor.memory", "2g")

    val ctx = new SparkContext(sparkConf)

    //lineage
    val lc = new LineageContext(ctx)
    lc.setCaptureLineage(lineage)
    //

    val lines = lc.textFile("../AdjacencyList/edges_31", 1)
    val edgesList = lines.flatMap(s => {
      val listofEdges: MutableList[(String, String)] = MutableList()
      val index: Int = s.lastIndexOf(",")
      if (index == -1) {
        System.out.println("Input in Wrong Format: " + s)
      }
      val outEdge: String = s.substring(0, index)
      val inEdge: String = s.substring(index + 1)
      val outList: String = "from{" + outEdge + "}:to{}"
      var inList: String = "from{}:to{" + inEdge + "}"
      //Fault Seeding
      if (outEdge.equals("VertexID00006510642_31")){
        inList = "from{}:to{VertexIDA}"
      }
      //
      val out: (String, String) = new (String, String)(outEdge, inList)
      val in: (String, String) = new (String, String)(inEdge, outList)
      listofEdges += out
      listofEdges += in
      listofEdges.toList
    })

    /* To see the result of the flapMapToPair function
    List<Tuple2<String, String>> output = edgesList.collect();
    for (Tuple2<?,?> tuple: output) {
        System.out.println(tuple._1() + ": " + tuple._2());
    }
    */
    val groupedEdgeList = edgesList.groupByKey

    val results = groupedEdgeList.map(strings => {
      val fromList: MutableList[String] = MutableList()
      val toList: MutableList[String] = MutableList()
      var str: String = new String
      var fromLine: String = new String
      var toLine: String = new String
      var vertex: String = new String
      //var r: Int = 0
      var strLength: Int = 0
      var index: Int = 0
      val itr: Iterator[String] = strings._2.iterator
      while (itr.hasNext) {
        breakable {
          str = itr.next
          strLength = str.length
          index = str.indexOf(":")
          if (index == -1) {
            System.out.println("Wrong Input: " + str)
            break
          }
          if (index > 6) fromLine = str.substring(5, index - 1)
          if (index + 5 < strLength) toLine = str.substring(index + 4, strLength - 1)
          if (!fromLine.isEmpty) {
            val itr2: StringTokenizer = new StringTokenizer(fromLine, ",")
            while (itr2.hasMoreTokens) {
              vertex = new String(itr2.nextToken)
              if (!fromList.contains(vertex) && fromList.size < LIMIT) fromList += vertex
            }
          }
          if (!toLine.isEmpty) {
            val itr2: StringTokenizer = new StringTokenizer(toLine, ",")
            while (itr2.hasMoreTokens) {
              vertex = new String(itr2.nextToken)
              if (!toList.contains(vertex) && toList.size < LIMIT) toList += vertex
            }
          }
        }
      }
      Collections.sort(fromList)
      Collections.sort(toList)
      var fromList_str: String = ""
      var toList_str: String = ""
      for (r <- fromList) {
        if (fromList_str.equals("")) fromList_str = r
        else fromList_str = fromList_str + "," + r
      }
      for (r<- toList) {
        if (toList_str.equals("")) toList_str = r
        else toList_str = toList_str + "," + r
      }
      val outValue: String = new String("from{" + fromList_str + "}:to{" + toList_str + "}")
      (strings._1, outValue)
    }).cache()

    results.collect

    lc.setCaptureLineage(false)
    Thread.sleep(1000)

    var linRdd = results.getLineage
    linRdd.collect.foreach(println)

    linRdd = linRdd.filter(18)
    linRdd.collect.foreach(println)
    linRdd = linRdd.goBackAll()
    linRdd.collect.foreach(println)

    println("Job's Done!")
    ctx.stop()

  }
}