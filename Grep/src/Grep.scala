/**
 * Created by Michael on 1/26/16.
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
import java.util.Calendar
import java.util.List
import java.util.StringTokenizer

import scala.collection.mutable.MutableList

//remove if not needed
import scala.collection.JavaConversions._

import scala.util.control.Breaks._
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._
import scala.sys.process._

object Grep {
//        def main(args:Array[String]): Unit = {
//          val sparkConf = new SparkConf().setMaster("local[8]")
//          sparkConf.setAppName("Grep_LineageDD-" )
//            .set("spark.executor.memory", "2g")
//
//          val ctx = new SparkContext(sparkConf)
//
//          val lines = ctx.textFile("../textFile", 1)
//          val constr = new sparkOperations
//          val output = constr.sparkWorks(lines, "am[eio]").collect
//          for (tuple <- output) {
//            println(tuple._1 + ": " + tuple._2)
//          }
//          ctx.stop()
//        }

  private val exhaustive = 0


  //function for testGroudTruth function
  def mapFunc(str: String): (String, Int) = {
    val token = new StringTokenizer(str)
    val exp = token.nextToken()
    val count = token.nextToken().toInt
    (exp, count)
  }


  def main(args: Array[String]): Unit = {
    try {
      //set up logging
      val lm: LogManager = LogManager.getLogManager
      val logger: Logger = Logger.getLogger(getClass.getName)
      val fh: FileHandler = new FileHandler("myLog")
      fh.setFormatter(new SimpleFormatter)
      lm.addLogger(logger)
      logger.setLevel(Level.INFO)
      logger.addHandler(fh)

      //set up spark configuration
      val sparkConf = new SparkConf().setMaster("local[8]")
      sparkConf.setAppName("Grep_LineageDD")
        .set("spark.executor.memory", "2g")

      //set up lineage
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

      //set up spark context
      val ctx = new SparkContext(sparkConf)


      //set up lineage context
      val lc = new LineageContext(ctx)
      lc.setCaptureLineage(lineage)


      //Prepare for Hadoop MapReduce for correctness test
      val clw = new commandLineOperations()
      clw.commandLineWorks()
      //Run Hadoop to have a groundTruth
      Seq("hadoop", "jar", "/Users/Michael/Documents/UCLA Senior/F15/Research-Fall2015/benchmark/examples/Grep.jar", "Grep1", "/Users/Michael/IdeaProjects/textFile", "output", "1","am[ei]").!!

      //start record timing for lineage
      val LineageStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      val LineageStartTime = System.nanoTime()
      logger.log(Level.INFO, "Record Lineage time starts at " + LineageStartTimestamp)

      //spark program starts here
      val lines = lc.textFile("../textFile", 1)
      val results = lines.flatMap(line => {
        val list: MutableList[String] = MutableList()
        val regex = "am[ei]".r
        val itr = regex.findAllMatchIn(line)
        while (itr.hasNext) {
          val value = itr.next()
          list += value.toString()
        }
        list.toIterable
      })
        .map(s => (s, 2))//Seeded fault, suppose to be (s, 1)
        .reduceByKey(_ + _)
      val out = results.collect()

      //lineage ends
      lc.setCaptureLineage(false)
      Thread.sleep(1000)

      val pw = new PrintWriter(new File("/Users/Michael/IdeaProjects/Grep/lineageResult"))

      val result = results.testGroundTruth[String, Int]("/Users/Michael/IdeaProjects/Grep/output/part-00000", mapFunc)
      var linRdd = results.getLineage()
      linRdd.collect

      linRdd = linRdd.filter { c => result.contains(c) }
      linRdd = linRdd.goBackAll()
      //At this stage, technically lineage has already find all the faulty data set, we record the time
      val lineageEndTime = System.nanoTime()
      val lineageEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      logger.log(Level.INFO, "Lineage takes " + (lineageEndTime - LineageStartTime) / 1000 + " microseconds")
      logger.log(Level.INFO, "Lineage ends at " + lineageEndTimestamp)


      linRdd.show.collect().foreach(s => {
        pw.append(s.toString)
        pw.append('\n')
      })

      pw.close()

      val lineageResult = ctx.textFile("/Users/Michael/IdeaProjects/Grep/lineageResult", 1)
      //val lineageResult = ctx.textFile("/Users/Michael/IdeaProjects/textFile", 1)

      val num = lineageResult.count()
      logger.log(Level.INFO, "Lineage caught " + num + " records to run delta-debugging")


      //Remove output before delta-debugging
      val outputFile = new File("/Users/Michael/IdeaProjects/Grep/output")
      if (outputFile.isDirectory) {
        for (list <- Option(outputFile.listFiles()); child <- list) child.delete()
      }
      outputFile.delete

      val DeltaDebuggingStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      val DeltaDebuggingStartTime = System.nanoTime()
      logger.log(Level.INFO, "Record DeltaDebugging (unadjusted) time starts at " + DeltaDebuggingStartTimestamp)

      /** **************
        * **********
        */
      //lineageResult.cache()

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
      logger.log(Level.INFO, "DeltaDebugging (unadjusted) ends at " + DeltaDebuggingEndTimestamp)
      logger.log(Level.INFO, "DeltaDebugging (unadjusted) takes " + (DeltaDebuggingEndTime - DeltaDebuggingStartTime) / 1000 + " milliseconds")


      //To print out the result
//      for (tuple <- results) {
//        println(tuple._1 + ": " + tuple._2)
//      }
      println("Job's DONE!")
      ctx.stop()
    }
  }
}
