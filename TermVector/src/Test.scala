/**
 * Created by Michael on 1/25/16.
 */

import java.util.StringTokenizer
import java.util.logging.{Level, Logger, FileHandler, LogManager}

import org.apache.spark.api.java.JavaRDD
import scala.sys.process._
import scala.io.Source

import java.io.File
import java.io._

class Test extends userTest[String] {
  def usrTest(inputRDD: JavaRDD[String], lm: LogManager, fh: FileHandler): Boolean = {
    val logger: Logger = Logger.getLogger(classOf[Test].getName)
    lm.addLogger(logger)
    logger.addHandler(fh)
    var returnValue = false
    val spw = new sparkOperations()
    val result = spw.sparkWorks(inputRDD)
    val output  = result.collect()
    val fileName = "/Users/Michael/IdeaProjects/TermVector/file2"
    val file = new File(fileName)

    val timeToAdjustStart: Long = System.nanoTime
    inputRDD.saveAsTextFile(fileName)
    Seq("hadoop", "jar", "/Users/Michael/Documents/UCLA Senior/F15/Research-Fall2015/benchmark/examples/TermVector.jar", "org.apache.hadoop.examples.TermVectorPerHost", "-m", "3", "-r", "1", fileName, "output").!!
    val timeToAdjustEnd: Long = System.nanoTime
    logger.log(Level.INFO, "Deduct " + (timeToAdjustEnd - timeToAdjustStart) / 1000 + " microseconds in this run to adjust")

    var truthList:Map[String, List[(String, Int)]] = Map()
    for(line <- Source.fromFile("/Users/Michael/IdeaProjects/TermVector/output/part-00000").getLines()) {
      val token = new StringTokenizer(line)
      val docName  = token.nextToken()
      val wordWithCount = token.nextToken()
      val colonIndex = wordWithCount.lastIndexOf(":")
      val word = wordWithCount.substring(0, colonIndex)
      val count = wordWithCount.substring(colonIndex + 1).toInt

      if (truthList.contains(docName)) {
        val newList = (word, count)::truthList(docName)
        truthList = truthList updated (docName, newList)
      } else {
        truthList = truthList updated (docName, List((word, count)))
      }
      //logger.log(Level.INFO, "TruthList[" + (truthList.size - 1) + "]: " + bin + " : "+ number)
    }

    val itr = output.iterator
    while (itr.hasNext) {
      val tupVal = itr.next()
      if (!truthList.contains(tupVal._1)) returnValue = true
      else {
        val itr2 = tupVal._2.toIterator
        while (itr2.hasNext){
          val tuple1 = itr2.next()
          if (!truthList(tupVal._1).contains(tuple1)) returnValue = true
          else {
            val updateList = truthList(tupVal._1).filter(_ != tuple1)
            truthList = truthList updated (tupVal._1, updateList)
          }
        }
        if (!truthList(tupVal._1).isEmpty) returnValue = true
        truthList = truthList - tupVal._1
      }
    }
    if (!truthList.isEmpty) returnValue = true

    val outputFile = new File("/Users/Michael/IdeaProjects/TermVector/output")

    if (file.isDirectory) {
      for (list <- Option(file.listFiles()); child <- list) child.delete()
    }
    file.delete
    if (outputFile.isDirectory) {
      for (list <- Option(outputFile.listFiles()); child <- list) child.delete()
    }
    outputFile.delete

    returnValue
  }
}
