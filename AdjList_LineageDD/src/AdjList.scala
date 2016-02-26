/**
 * Created by Michael on 11/23/15.
 */
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
object AdjList {
  val LIMIT: Int = 200000
  private val exhaustive = 0

  def mapFunc(str: String): (String, String) = {
    val token = new StringTokenizer(str)
    val edge = token.nextToken()
    val lists = token.nextToken()
    return (edge, lists)
  }

  def main(args: Array[String]): Unit = {
    try {
      val lm: LogManager = LogManager.getLogManager
      val logger: Logger = Logger.getLogger(getClass.getName)
      val fh: FileHandler = new FileHandler("myLog")
      fh.setFormatter(new SimpleFormatter)

      lm.addLogger(logger)
      logger.setLevel(Level.INFO)

      logger.addHandler(fh)

      val LineageStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      val LineageStartTime = System.nanoTime()
      logger.log(Level.INFO, "Record Lineage time starts at " + LineageStartTimestamp)

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

      sparkConf.setAppName("AdjacencyList_lineageDD-" + lineage + "-" + logFile)
        .set("spark.executor.memory", "2g")

      val ctx = new SparkContext(sparkConf)

      //lineage
      val lc = new LineageContext(ctx)
      lc.setCaptureLineage(lineage)
      //


      //Prepare for Hadoop MapReduce
      val clw = new commandLineOperations()
      clw.commandLineWorks()
      //Run Hadoop to have a groundTruth
      Seq("hadoop", "jar", "/Users/Michael/Documents/UCLA Senior/F15/Research-Fall2015/benchmark/examples/AdjList.jar", "org.apache.hadoop.examples.AdjList", "/Users/Michael/IdeaProjects/AdjacencyList/edges_31", "output").!!

      val lines = lc.textFile("../AdjacencyList/edges_31", 1)
      logger.log(Level.INFO, "Total data set size is " + lines.count)


      //Seeded fault operation
      val filteredList = lines.filter(s => {
        if (s.substring(14, 19) == "10642") false
        else true
      })

      //

      val edgesList = filteredList.flatMap(s => {
          val listofEdges: MutableList[(String, String)] = MutableList()
          val index: Int = s.lastIndexOf(",")
          if (index == -1) {
            System.out.println("Input in Wrong Format: " + s)
          }
          val outEdge: String = s.substring(0, index)
          val inEdge: String = s.substring(index + 1)
          val outList: String = "from{" + outEdge + "}:to{}"
          val inList: String = "from{}:to{" + inEdge + "}"
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

      val results = groupedEdgeList.mapValues(strings => {
          val fromList: MutableList[String] = MutableList()
          val toList: MutableList[String] = MutableList()
          var str: String = new String
          var fromLine: String = new String
          var toLine: String = new String
          var vertex: String = new String
          //var r: Int = 0
          var strLength: Int = 0
          var index: Int = 0
          val itr: Iterator[String] = strings.iterator
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
          outValue
      }).cache()

      results.count

      lc.setCaptureLineage(false)
      Thread.sleep(1000)

      val pw = new PrintWriter(new File("/Users/Michael/IdeaProjects/AdjList_LineageDD/lineageResult"))

      val result = results.testGroundTruth[String, String]("/Users/Michael/IdeaProjects/AdjList_LineageDD/output/part-00000", mapFunc)
      var linRdd = results.getLineage()
      linRdd.collect
      //println("The result list is: " + result)

      for (c <- result) {
        linRdd = linRdd.filter(c)
        linRdd = linRdd.goBackAll()
        linRdd.show.collect().foreach(s => {
          pw.append(s.toString)
          pw.append('\n')
        })
        linRdd = results.getLineage()
        linRdd.collect
      }
      //At this stage, technically lineage has already find all the faulty data set, we record the time
      val lineageEndTime = System.nanoTime()
      val lineageEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      logger.log(Level.INFO, "Lineage takes " + (lineageEndTime - LineageStartTime)/1000 + " milliseconds")
      logger.log(Level.INFO, "Lineage ends at " + lineageEndTimestamp)


      pw.close()

      val lineageResult = ctx.textFile("/Users/Michael/IdeaProjects/AdjList_LineageDD/lineageResult", 1)
      val num = lineageResult.count()
      logger.log(Level.INFO, "Lineage caught " + num + " records to run delta-debugging")

      //Remove output before delta-debugging
      val outputFile = new File("/Users/Michael/IdeaProjects/AdjList_LineageDD/output")
      if (outputFile.isDirectory) {
        for (list <- Option(outputFile.listFiles()); child <- list) child.delete()
      }
      outputFile.delete

      val DeltaDebuggingStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      val DeltaDebuggingStartTime = System.nanoTime()
      logger.log(Level.INFO, "Record DeltaDebugging time starts at " + DeltaDebuggingStartTimestamp)

      if (exhaustive == 1) {
        val delta_debug: DD[String] = new DD[String]
        delta_debug.ddgen(lineageResult, new Test,
          new Split, lm, fh)
      } else {
        val delta_debug: DD_NonEx[String] = new DD_NonEx[String]
        delta_debug.ddgen(lineageResult, new Test, new Split, lm, fh)
      }

      val DeltaDebuggingEndTime = System.nanoTime()
      val DeltaDebuggingEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      logger.log(Level.INFO, "DeltaDebugging ends at " + DeltaDebuggingEndTimestamp)
      logger.log(Level.INFO, "DeltaDebugging takes " + (DeltaDebuggingEndTime - DeltaDebuggingStartTime)/1000 + " milliseconds")


      //To print out the result
      //    for (tuple <- output) {
      //      println(tuple._1 + ": " + tuple._2)
      //    }

      println("Job's DONE!")
      ctx.stop()

    }
  }
}
