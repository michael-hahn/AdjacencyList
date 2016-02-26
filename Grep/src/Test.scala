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
    val result = spw.sparkWorks(inputRDD, "am[ei]")
    val output  = result.collect()
    val fileName = "/Users/Michael/IdeaProjects/Grep/file2"
    val file = new File(fileName)

    val timeToAdjustStart: Long = System.nanoTime
    inputRDD.saveAsTextFile(fileName)
    Seq("hadoop", "jar", "/Users/Michael/Documents/UCLA Senior/F15/Research-Fall2015/benchmark/examples/Grep.jar", "Grep1", fileName, "output", "1","am[ei]").!!
    val timeToAdjustEnd: Long = System.nanoTime
    logger.log(Level.INFO, "Deduct " + (timeToAdjustEnd - timeToAdjustStart) / 1000 + " microseconds in this run to adjust")

    var truthList:Map[String, Int] = Map()
    for(line <- Source.fromFile("/Users/Michael/IdeaProjects/Grep/output/part-00000").getLines()) {
      val token = new StringTokenizer(line)
      val exp  = token.nextToken()
      val count = token.nextToken().toInt
      truthList = truthList + (exp -> count)
      //logger.log(Level.INFO, "TruthList[" + (truthList.size - 1) + "]: " + bin + " : "+ number)
    }


    val itr = output.iterator
    while (itr.hasNext) {
      val tupVal = itr.next()
      val outputWord = tupVal._1
      val outputCount = tupVal._2
      if (!truthList.contains(outputWord)) returnValue = true
      else{
        if (!truthList(outputWord).equals(outputCount)) returnValue = true
        else truthList = truthList - outputWord
      }
    }
    if (!truthList.isEmpty) returnValue = true

    val outputFile = new File("/Users/Michael/IdeaProjects/Grep/output")

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