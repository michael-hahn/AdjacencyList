/**
 * Created by Michael on 11/13/15.
 */

import java.util.StringTokenizer
import java.util.logging.{Level, Logger, FileHandler, LogManager}

import org.apache.spark.api.java.JavaRDD
import org.apache.spark.SparkContext._
import org.apache.spark.lineage.rdd.ShowRDD
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import scala.collection.mutable
import scala.collection.mutable.MutableList
import scala.sys.process._
import scala.io.Source
import scala.util.control.Breaks._

import java.io.File
import java.io._





class Test extends userTest[String] with Serializable {
  val LIMIT: Int = 200000

  def usrTest(inputRDD: RDD[String], lm: LogManager, fh: FileHandler): Boolean = {
    //use the same logger as the object file
    val logger: Logger = Logger.getLogger(classOf[Test].getName)
    lm.addLogger(logger)
    logger.addHandler(fh)

    //assume that test will pass (which returns false)
    var returnValue = false

    /*
    val spw = new sparkOperations()
    val result = spw.sparkWorks(inputRDD)
    val output  = result.collect()
    val fileName = "/Users/Michael/IdeaProjects/AdjacencyList/file2"
    val file = new File(fileName)

    val timeToAdjustStart: Long = System.nanoTime
    inputRDD.saveAsTextFile(fileName)
    Seq("hadoop", "jar", "/Users/Michael/Documents/UCLA Senior/F15/Research-Fall2015/benchmark/examples/AdjList.jar", "org.apache.hadoop.examples.AdjList", "-m", "3", "-r", "1", fileName, "output").!!
    val timeToAdjustEnd:Long = System.nanoTime
    logger.log(Level.INFO, "Deduct " + (timeToAdjustEnd - timeToAdjustStart) / 1000 + " microseconds in this run to adjust")

    var truthList: Map[String, (List[String], List[String])] = Map()
    for(line <- Source.fromFile("/Users/Michael/IdeaProjects/AdjList_FaultSeeding/output/part-00000").getLines()) {
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


    val itr = output.iterator
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

    val outputFile = new File("/Users/Michael/IdeaProjects/AdjList_FaultSeeding/output")

    if (file.isDirectory) {
      for (list <- Option(file.listFiles()); child <- list) child.delete()
    }
    file.delete
    if (outputFile.isDirectory) {
      for (list <- Option(outputFile.listFiles()); child <- list) child.delete()
    }
    outputFile.delete
    return returnValue
    */
    val resultRDD = inputRDD.map(s => {
        val str = s.toString
        println(str)
        val index = str.lastIndexOf(",")
        val content = str.substring(2, index - 1)
        val index2 = content.indexOf(",")
        val key = content.substring(0, index2)
        val value = content.substring(index2 + 1)
        (key, value)
      })
      .groupByKey()
      .map(pair => {
      var fromList: MutableList[String] = MutableList()
      var toList: MutableList[String] = MutableList()
      var fromLine = new String()
      var toLine = new String()
      var vertex = new String()
      //       var itr:util.Iterator[String] = null
      //        try {
      val itr = pair._2.toIterator
      //       }catch{
      //         case e:Exception =>
      //           println("**************************")
      //       }
      while (itr.hasNext) {
        breakable {
          val str = itr.next()
          val strLength = str.length
          val index = str.indexOf(":")
          if (index == -1) {
            println("Wrong input: " + str)
            break
          }
          if (index > 6) {
            fromLine = str.substring(5, index - 1)
          }
          if (index + 5 < strLength) {
            toLine = str.substring(index + 4, strLength - 1)
          }
          if (!fromLine.isEmpty) {
            val itr2 = new StringTokenizer(fromLine, ",")
            while (itr2.hasMoreTokens) {
              vertex = new String(itr2.nextToken())
              if (!fromList.contains(vertex) && fromList.size < LIMIT) {
                fromList += vertex
              }
            }
          }
          if (!toLine.isEmpty) {
            val itr2 = new StringTokenizer(toLine, ",")
            while (itr2.hasMoreTokens) {
              vertex = new String(itr2.nextToken())
              if (!toList.contains(vertex) && toList.size < LIMIT) {
                toList += vertex
              }
            }
          }
        }
      }
      fromList = fromList.sortWith((a, b) => if (a < b) true else false)
      toList = toList.sortWith((a, b) => if (a < b) true else false)
      var fromList_str = new String("")
      var toList_str = new String("")
      for (r <- 0 until fromList.size) {
        if (fromList_str.equals("")) fromList_str = fromList(r)
        else fromList_str = fromList_str + "," + fromList(r)
      }
      for (r <- 0 until toList.size) {
        if (toList_str.equals("")) toList_str = toList(r)
        else toList_str = toList_str + "," + toList(r)
      }
      val outValue = new String("from{" + fromList_str + "}:to{" + toList_str + "}")
      (pair._1, outValue)
    })
      .map(pair => {
        val index = pair._2.lastIndexOf(":")
        val substr = pair._2.substring(index + 4, pair._2.length - 1)
        val ll = substr.split(",")
        var value = pair._2
        if (ll.size > 438) {
          value += "*"
        }
        (pair._1, value)
      })

    val out = resultRDD.collect()

    for (o <- out) {
//      logger.log(Level.INFO, o._1 + ": " + o._2)
      if (o._2.substring(o._2.length - 1).equals("*")) returnValue = true
    }
    logger.log(Level.INFO, "************************ One round of Test Done ***************************")
    returnValue
  }
}

