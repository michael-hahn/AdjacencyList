/**
 * Created by Michael on 11/13/15.
 */

import java.util.StringTokenizer
import java.util.logging.{Level, Logger, FileHandler, LogManager}

import org.apache.spark.api.java.JavaRDD
import scala.sys.process._
import scala.io.Source

import java.io.File
import java.io._





class Test extends userTest[String] {

  def usrTest(inputRDD: JavaRDD[String], maxClusters: Int, totalClusters: Int, centroid_ref: Array[Cluster], lm: LogManager, fh: FileHandler): Boolean = {
    val logger: Logger = Logger.getLogger(classOf[Test].getName)
    lm.addLogger(logger)
    logger.addHandler(fh)
    var returnValue = false
    val spw = new sparkOperations()
    val result = spw.sparkWorks(inputRDD, maxClusters, totalClusters, centroid_ref)
    val output  = result.collect()
    val fileName = "/Users/Michael/IdeaProjects/kmeans/file2"
    val file = new File(fileName)

    val timeToAdjustStart: Long = System.nanoTime
    inputRDD.saveAsTextFile(fileName)
    Seq("hadoop", "jar", "/Users/Michael/Documents/UCLA Senior/F15/Research-Fall2015/benchmark/examples/kmeans.jar", "org.apache.hadoop.examples.kmeans.Kmeans", "-m", "3", "-r", "1", fileName, "output").!!
    val timeToAdjustEnd: Long = System.nanoTime
    logger.log(Level.INFO, "Deduct " + (timeToAdjustEnd - timeToAdjustStart) / 1000 + " microseconds in this run to adjust")

    var truthList:Map[Int, List[String]] = Map()
    for(line <- Source.fromFile("/Users/Michael/IdeaProjects/kmeans/output/part-00000").getLines()) {
      val token = new StringTokenizer(line.trim)
      val key  = token.nextToken().toInt
      var value = new String("")
      while (token.hasMoreTokens) {
        value += token.nextToken
        value += " "
      }
      if (value.substring(value.size - 1).equals(" ")) {
        value = value.substring(0, value.size - 1)
      }
      if (truthList.contains(key)) {
        var newList = value::truthList(key)
        truthList = truthList updated (key, newList)
      } else {
        truthList = truthList updated (key, List(value))
      }
      //logger.log(Level.INFO, "TruthList[" + (truthList.size - 1) + "]: " + bin + " : "+ number)
    }


    val itr = output.iterator
    while (itr.hasNext) {
      val tupVal = itr.next()
      if (!truthList.contains(tupVal._1)) returnValue = true
      else {
        val value = tupVal._2
        if (!truthList(tupVal._1).contains(value)) {
          returnValue = true
        } else {
          val updateList = truthList(tupVal._1).filter(_ != value)
          truthList = truthList updated (tupVal._1, updateList)
        }
      }
    }
    val itr2 = truthList.toIterator
    while (itr2.hasNext) {
      val tupVal2 = itr2.next()
      val mapVal = tupVal2._2
      if (mapVal.isEmpty) {
        truthList = truthList - tupVal2._1
      } else {
        returnValue = true
      }
    }
    if (!truthList.isEmpty) returnValue = true

    val outputFile = new File("/Users/Michael/IdeaProjects/kmeans/output")

    if (file.isDirectory) {
      for (list <- Option(file.listFiles()); child <- list) child.delete()
    }
    file.delete
    if (outputFile.isDirectory) {
      for (list <- Option(outputFile.listFiles()); child <- list) child.delete()
    }
    outputFile.delete
    return returnValue
  }
}
