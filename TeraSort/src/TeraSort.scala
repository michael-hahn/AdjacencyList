/**
 * Created by Michael on 1/21/16.
 */

import java.io.{PrintWriter, File}
import java.lang.Exception
import java.util.logging._
import org.apache.spark.{rdd, SparkConf, SparkContext}
import org.apache.spark.SparkContext._
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

object TeraSort {
    def main(args: Array[String]): Unit = {
      val sparkConf = new SparkConf().setMaster("local[8]")
        .setAppName("Terasort")
        .set("spark.executor.memory", "2g")

      val ctx = new SparkContext(sparkConf)

      val lines = ctx.textFile("/Users/Michael/IdeaProjects/TeraGen/TeraGen", 1)
      val result = lines.map(s=>{
        val token = new StringTokenizer(s)
        val key = token.nextToken()
        val value = token.nextToken()
        (key, value)
      })
      .sortByKey()
      .collect()
      .foreach(println)

    }


}
