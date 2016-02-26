/**
 * Created by Michael on 11/12/15.
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

object Histogram_Ratings {
  private val exhaustive = 0

  def mapFunc(str: String): (Int, Int) = {
    val token = new StringTokenizer(str)
    val bin = token.nextToken().toInt
    val value = token.nextToken().toInt
    return (bin, value)
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

      //      val LineageStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      //      val LineageStartTime = System.nanoTime()
      //      logger.log(Level.INFO, "Record Lineage time starts at " + LineageStartTimestamp)

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

      sparkConf.setAppName("Histogram_ratings_lineageComplete-" + lineage + "-" + logFile)
        .set("spark.executor.memory", "2g")

      val ctx = new JavaSparkContext(sparkConf)

      //lineage
      val lc = new LineageContext(ctx)
      lc.setCaptureLineage(lineage)
      //


      //Prepare for Hadoop MapReduce
      val clw = new commandLineOperations()
      clw.commandLineWorks()
      //Run Hadoop to have a groundTruth
      Seq("hadoop", "jar", "/Users/Michael/Documents/UCLA Senior/F15/Research-Fall2015/benchmark/examples/HistogramRatings.jar", "org.apache.hadoop.examples.HistogramRatings", "/Users/Michael/IdeaProjects/Classification/file1s", "output").!!

      val LineageStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      val LineageStartTime = System.nanoTime()
      logger.log(Level.INFO, "Record Lineage time starts at " + LineageStartTimestamp)

      val lines = lc.textFile("../Classification/file1s", 1)

      val ratings  = lines.flatMap(s => {
        val intList: MutableList[Integer] = MutableList()
        var rating: Int = 0
        var reviewIndex: Int = 0
        var movieIndex: Int = 0
        var reviews: String = new String
        var tok: String = new String
        var ratingStr: String = new String
        var raterStr: String = new String
        movieIndex = s.indexOf(":")
        if (movieIndex > 0) {
          reviews = s.substring(movieIndex + 1)
          val token: StringTokenizer = new StringTokenizer(reviews, ",")
          while (token.hasMoreTokens) {
            tok = token.nextToken
            reviewIndex = tok.indexOf("_")
            raterStr = tok.substring(0, reviewIndex)
            ratingStr = tok.substring(reviewIndex + 1)
            rating = ratingStr.toInt
            var rater = raterStr.toLong
            if (rating == 1 || rating == 2) {
              if (rater % 13 != 0) {
                intList += rating
              }
            }
            else intList += rating
          }
        }
        intList.toIterable
      })
      val ones = ratings.map(integer => (integer, 1))
      val counts  = ones.reduceByKey( _ + _ )

      counts.count
      for (c <- counts) {
        println(c._1 + ":" + c._2)
      }

      lc.setCaptureLineage(false)
      Thread.sleep(1000)

      val pw = new PrintWriter(new File("/Users/Michael/IdeaProjects/Histogram_Ratings_LineageComplete/lineageResult"))

      val result = counts.testGroundTruth[Int, Int]("/Users/Michael/IdeaProjects/Histogram_Ratings_LineageComplete/output/part-00000", mapFunc)

      println("**************************")
      for(r <- result) {
        println(r)
      }
      println("**************************")

      var linRdd = counts.getLineage()
      linRdd.collect

      linRdd = linRdd.filter{c =>
        println("***" + c + "***")
        result.contains(c)}
      linRdd = linRdd.goBackAll()
      //At this stage, technically lineage has already find all the faulty data set, we record the time
      val lineageEndTime = System.nanoTime()
      val lineageEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      logger.log(Level.INFO, "Lineage takes " + (lineageEndTime - LineageStartTime)/1000 + " milliseconds")
      logger.log(Level.INFO, "Lineage ends at " + lineageEndTimestamp)

      linRdd = linRdd.goNext()


      linRdd.show.collect.foreach(println)


//      linRdd.show.collect().foreach(s => {
//        pw.append(s.toString)
//        pw.append('\n')
//      })

      pw.close()

      /*
      val lineageResult = ctx.textFile("/Users/Michael/IdeaProjects/Histogram_Ratings_LineageComplete/lineageResult", 1)
      val num = lineageResult.count()
      logger.log(Level.INFO, "Lineage caught " + num + " records to run delta-debugging")

      //Remove output before delta-debugging
      val outputFile = new File("/Users/Michael/IdeaProjects/Histogram_Ratings_LineageComplete/output")
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
      lineageResult.cache()

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
      logger.log(Level.INFO, "DeltaDebugging (unadjusted) takes " + (DeltaDebuggingEndTime - DeltaDebuggingStartTime)/1000 + " milliseconds")


      //To print out the result
      //    for (tuple <- output) {
      //      println(tuple._1 + ": " + tuple._2)
      //    }
      */
      println("Job's DONE!")
      ctx.stop()

    }
  }
}
