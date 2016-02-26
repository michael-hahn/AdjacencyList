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

  def usrTest(inputRDD: JavaRDD[String], lm: LogManager, fh: FileHandler): Boolean = {
    val logger: Logger = Logger.getLogger(classOf[Test].getName)
    lm.addLogger(logger)
    logger.addHandler(fh)
    var returnValue = false
    val spw = new sparkOperations()
    val result = spw.sparkWorks(inputRDD)
    val output  = result.collect()
    val fileName = "/Users/Michael/IdeaProjects/RankedInvertedIndex/file2"
    val file = new File(fileName)

    val timeToAdjustStart: Long = System.nanoTime
    inputRDD.saveAsTextFile(fileName)
    Seq("hadoop", "jar", "/Users/Michael/Documents/UCLA Senior/F15/Research-Fall2015/benchmark/examples/RankedInvertedIndex.jar", "org.apache.hadoop.examples.RankedInvertedIndex", "-m", "3", "-r", "1",fileName, "output").!!
    val timeToAdjustEnd: Long = System.nanoTime
    logger.log(Level.INFO, "Deduct " + (timeToAdjustEnd - timeToAdjustStart) / 1000 + " microseconds in this run to adjust")

    var truthList:Map[String, List[String]] = Map()
    for(line <- Source.fromFile("/Users/Michael/IdeaProjects/RankedInvertedIndex/output/part-00000").getLines()) {
      val token = new StringTokenizer(line)
      val word  = token.nextToken()
      val docID = token.nextToken()
      if (truthList.contains(word)) {
        var newList = docID::truthList(word)
        newList = newList.reverse
        truthList = truthList updated (word, newList)
      } else {
        truthList = truthList updated (word, List(docID))
      }
      //logger.log(Level.INFO, "TruthList[" + (truthList.size - 1) + "]: " + bin + " : "+ number)
    }


    val itr = output.iterator
    while (itr.hasNext) {
      val tupVal = itr.next()
      if (!truthList.contains(tupVal._1)) returnValue = true
      else {
        val itr2 = tupVal._2.toIterator
        while (itr2.hasNext) {
          val docName = itr2.next()
          if (!truthList(tupVal._1).contains(docName)) {
            returnValue = true
          } else {
            val updateList = truthList(tupVal._1).filter(_ != docName)
            truthList = truthList updated (tupVal._1, updateList)
          }
        }
        if (!truthList(tupVal._1).isEmpty) returnValue = true
        truthList = truthList - tupVal._1
      }
    }
    if (!truthList.isEmpty) returnValue = true

    val outputFile = new File("/Users/Michael/IdeaProjects/RankedInvertedIndex/output")

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
