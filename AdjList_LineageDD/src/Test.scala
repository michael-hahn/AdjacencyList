/**
 * Created by Michael on 11/13/15.
 */

import java.util.StringTokenizer
import java.util.logging.{Level, Logger, FileHandler, LogManager}

import org.apache.spark.api.java.JavaRDD
import scala.collection.mutable
import scala.sys.process._
import scala.io.Source
import scala.util.control.Breaks._

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
    val fileName = "/Users/Michael/IdeaProjects/AdjacencyList/file2"
    val file = new File(fileName)

    inputRDD.saveAsTextFile(fileName)
    Seq("hadoop", "jar", "/Users/Michael/Documents/UCLA Senior/F15/Research-Fall2015/benchmark/examples/AdjList.jar", "org.apache.hadoop.examples.AdjList", fileName, "output").!!

    var truthList: Map[String, (List[String], List[String])] = Map()
    for(line <- Source.fromFile("/Users/Michael/IdeaProjects/AdjList_LineageDD/output/part-00000").getLines()) {
      val token = new StringTokenizer(line)
      val edge :String = token.nextToken()
      val edgeListsStr :String = token.nextToken()
      val fromIndex = edgeListsStr.indexOf("{")
      val toIndex = edgeListsStr.indexOf(":")
      val fromListStr = edgeListsStr.substring(fromIndex + 1, toIndex - 1)
      val toListStr = edgeListsStr.substring(toIndex + 4, edgeListsStr.size - 1)
      val fromList = fromListStr.split(",").toList
      val to_list = toListStr.split(",").toList
      truthList += (edge -> (fromList, to_list))
      //logger.log(Level.INFO, "TruthList[" + (truthList.size - 1) + "]: " + bin + " : "+ number)
    }


    val itr = output.iterator()
    breakable {
      while (itr.hasNext) {
        val tupVal = itr.next()
        val edgeStr = tupVal._1
        val edgeListOutputStr = tupVal._2
        val outputFromIndex = edgeListOutputStr.indexOf("{")
        val outputToIndex = edgeListOutputStr.indexOf(":")
        val outputFromListStr = edgeListOutputStr.substring(outputFromIndex + 1, outputToIndex - 1)
        val outputToListStr = edgeListOutputStr.substring(outputToIndex + 4, edgeListOutputStr.size - 1)
        val outputFromList = outputFromListStr.split(",").toList
        val outputToList = outputToListStr.split(",").toList
        //logger.log(Level.INFO, "Output: " + binName + " : " + num)
        if (truthList.contains(edgeStr)) {
          val truthPair = truthList(edgeStr)
          if (outputFromList.contains("") && !truthPair._1.contains("")) {
            returnValue = true
            break
          }
          if (outputToList.contains("") && !truthPair._2.contains("")) {
            returnValue = true
            break
          }
          if (truthPair._1.contains("") && !outputFromList.contains("")) {
            returnValue = true
            break
          }
          if (!outputToList.contains("") && truthPair._2.contains("")) {
            returnValue = true
            break
          }
          for (elem <- outputFromList) {
            if (!truthPair._1.contains(elem)) {
              returnValue = true
              break
            }
          }
          for (elem <- outputToList) {
            if (!truthPair._2.contains(elem)) {
              returnValue = true
              break
            }
          }
          for (elem <- truthPair._1) {
            if (!outputFromList.contains(elem)) {
              returnValue = true
              break
            }
          }
          for (elem <- truthPair._2){
            if (!outputToList.contains(elem)) {
              returnValue = true
              break
            }
          }
        } else {
          returnValue = true
          break
        }
      }
    }

    val outputFile = new File("/Users/Michael/IdeaProjects/AdjList/output")

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

