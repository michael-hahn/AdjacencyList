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
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import scala.Tuple2
import java.util.{Scanner, Calendar, StringTokenizer}

import scala.collection.mutable.MutableList
import scala.io.Source
import scala.reflect.ClassTag

//remove if not needed
import scala.collection.JavaConversions._

import scala.util.control.Breaks._
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._

import org.apache.spark.SparkContext._
import scala.sys.process._

object RankedInvertedIndex {
//      def main(args:Array[String]): Unit = {
//        val sparkConf = new SparkConf().setMaster("local[8]")
//        sparkConf.setAppName("rankedInvertedIndex_LineageDD-" )
//          .set("spark.executor.memory", "2g")
//
//        val ctx = new SparkContext(sparkConf)
//
//        val pw = new PrintWriter(new File("/Users/Michael/Desktop/RII"))
//
//
//        val lines = ctx.textFile("/Users/Michael/Documents/UCLA Senior/F15/Research-Fall2015/output_SequenceCount/part-00000", 1)
//        val constr = new sparkOperations
//        val output = constr.sparkWorks(lines).collect
//        val itr = output.iterator
//        while (itr.hasNext) {
//          val tupVal = itr.next()
//          pw.append(tupVal._1 + ": --- \n")
//          val itr2 = tupVal._2.toIterator
//          while (itr2.hasNext){
//            val strVal = itr2.next()
//            pw.append(strVal + " \n")
//          }
//
//        }
//        pw.close()
//        ctx.stop()
//      }
  private val exhaustive = 0



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
      sparkConf.setAppName("RankedInvertedIndex_LineageDD")
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


      val ctx = new SparkContext(sparkConf)

      //set up lineage context and start capture lineage in the following spark program
      val lc = new LineageContext(ctx)
      lc.setCaptureLineage(true)


      /* For correctness test only (produce the ground truth file)
      //Prepare for Hadoop MapReduce
      val clw = new commandLineOperations()
      clw.commandLineWorks()
      //Run Hadoop to have a groundTruth
      Seq("hadoop", "jar", "/Users/Michael/Documents/UCLA Senior/F15/Research-Fall2015/benchmark/examples/RankedInvertedIndex.jar", "org.apache.hadoop.examples.RankedInvertedIndex", "-m", "3", "-r", "1","/Users/Michael/Documents/UCLA Senior/F15/Research-Fall2015/output_SequenceCount/rii_file", "output").!!
      */

      //start recording the time for lineage
      val LineageStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      val LineageStartTime = System.nanoTime()
      logger.log(Level.INFO, "Record Lineage time starts at " + LineageStartTimestamp)

      val lines = lc.textFile("/Users/Michael/Documents/UCLA Senior/F15/Research-Fall2015/output_SequenceCount/rii", 1)

      val separate = lines
        //map to key-value pair of (wordString, (docId, count))
        .map(line => {
        val countIndex = line.lastIndexOf("\t")
        val valueString = line.substring(0, countIndex)
        val count = line.substring(countIndex + 1).toInt

        val fileIndex = valueString.lastIndexOf("|")
        val wordString = valueString.substring(0, fileIndex)
        val docId = valueString.substring(fileIndex + 1)

        //val fileCountPair = new FileCountPair(docId, count)
        val fileCountPair = new String(docId + "-" + count)
        (wordString, fileCountPair)
      })
        //.groupByKey()
        .reduceByKey(_ + ";" + _)
        //This map sort the documents based on the counts in a descending order
        .map(pair => {
          //val valueList: MutableList[FileCountPair] = MutableList()
          val valueList: MutableList[String] = MutableList()
          val resultList: MutableList[String] = MutableList()
          //val itr = pair._2.toIterator
          val itr = pair._2.split(";").toIterator
          while (itr.hasNext) {
            //val valuePair = new FileCountPair(itr.next())
            val valuePair = new String(itr.next())
            valueList += valuePair
          }
          val valueListArr = valueList.toArray
          val newList = valueListArr.sortWith((a, b) => {
            //if (a.getCount > b.getCount) true
            //else false
            val aColon = a.indexOf("-")
            val bColon = b.indexOf("-")
            val aCount = a.substring(aColon + 1).toInt
            val bCount = b.substring(bColon + 1).toInt
            if (aCount > bCount) true
            else false
          })
          for (a <- newList) {
            val colonIndex = a.indexOf("-")
            val cnt = a.substring(colonIndex + 1)
            //val cnt = a.getCount
            val file = a.substring(0, colonIndex)
            //val rst = cnt + "|" + a.getFile
            val rst = cnt + "|" + file
            resultList += rst
          }
          (pair._1, resultList.toList)
        })
        //this map creates an exception
        .map(pair => {
          var value = new String("")
          for (s <- pair._2) {
            value += s + ","
          }
          value = value.substring(0, value.length - 1)
          if (pair._2.size > 1) {
            value += "*"
          }
          (pair._1, value)
        })

      val out = separate.collectWithId()

      lc.setCaptureLineage(false)
      Thread.sleep(1000)

      //print out the result of the spark program - for debugging purpose

      for (o <- out) {
        println(o._1._1 + ": " + o._1._2 + " - " + o._2)
      }


      //get the index of results that crashed the program
      var list = List[Long]()
      for (o <- out) {
        if (o._1._2.substring(o._1._2.length - 1).equals("*")) {
          list = o._2 :: list
        }
      }

      //print out the resulting list for debugging purpose
//      println("************************")
//      for (i <- list){
//        println(i)
//      }
//      println("************************")


      val pw = new PrintWriter(new File("/Users/Michael/IdeaProjects/RankedInvertedIndex/lineageResult"))


      var linRdd = separate.getLineage()
      linRdd.collect

      println("**********************************")
      linRdd = linRdd.filter(l => {
        //println("***"+l+"***")
        list.contains(l)
      })
      //linRdd = linRdd.filter(0)

      linRdd = linRdd.goBackAll()
      //At this stage, technically lineage has already find all the faulty data set, we record the time
      val lineageEndTime = System.nanoTime()
      val lineageEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      logger.log(Level.INFO, "Lineage takes " + (lineageEndTime - LineageStartTime)/1000 + " microseconds")
      logger.log(Level.INFO, "Lineage ends at " + lineageEndTimestamp)

//      linRdd.show.collect.foreach(println)

      linRdd.show.collect().foreach(s => {
        pw.append(s.toString)
        pw.append('\n')
      })

      pw.close()

/*
      //val lineageResult = ctx.textFile("/Users/Michael/IdeaProjects/InvertedIndex/lineageResult", 1)
      val lineageResult = ctx.textFile("/Users/Michael/Documents/UCLA Senior/F15/Research-Fall2015/output_SequenceCount/rii_file", 1)

      val num = lineageResult.count()
      logger.log(Level.INFO, "Lineage caught " + num + " records to run delta-debugging")


      //Remove output before delta-debugging
      val outputFile = new File("/Users/Michael/IdeaProjects/RankedInvertedIndex/output")
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
      logger.log(Level.INFO, "DeltaDebugging (unadjusted) takes " + (DeltaDebuggingEndTime - DeltaDebuggingStartTime)/1000 + " microseconds")


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
