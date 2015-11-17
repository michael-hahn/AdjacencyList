/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples.lineage

import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._


object stdDDWithLineage {

  def mapFunc(str: String): (String, Int) = {
    val substr = str.substring(1, str.size - 1)
    val elems = substr.split(" ")
    return (elems(0), elems(1).toInt)
  }

  def map1(elem: Double): (String, Double) = {
    if (elem < 100) return ("Small", elem)
    else if (100 <= elem && elem < 1000) return ("Medium", elem)
    else return ("Large", elem)
  }

  def main(args: Array[String]) {
    val conf = new SparkConf()
    var lineage = true
    var logFile = "hdfs://scai01.cs.ucla.edu:9000/clash/data/"
    if(args.size < 2) {
      logFile = "numberTest"
      conf.setMaster("local[1]")
      lineage = true
    } else {
      lineage = args(0).toBoolean
      logFile += args(1)
      conf.setMaster("spark://SCAI01.CS.UCLA.EDU:7077")
      //      conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //      conf.set("spark.kryo.referenceTracking", "false")
      //      conf.set("spark.kryo.registrationRequired", "true")
      //      conf.registerKryoClasses(Array(
      //        classOf[RoaringBitmap],
      //        classOf[BitmapContainer],
      //        classOf[RoaringArray],
      //        classOf[RoaringArray.Element],
      //        classOf[ArrayContainer],
      //        classOf[Array[RoaringArray.Element]],
      //        classOf[Array[Tuple2[_, _]]],
      //        classOf[Array[Short]],
      //        classOf[Array[Int]],
      //        classOf[Array[Long]],
      //        classOf[Array[Object]]
      //      ))
    }
    conf.setAppName("StandardDeviationWithDeltaDebuggingAndLineage-" + lineage + "-" + logFile)

    val sc = new SparkContext(conf)
    val lc = new LineageContext(sc)

    lc.setCaptureLineage(lineage)

    // Job
    val file: RDD[String] = lc.textFile(logFile)
    val numbers: RDD[Double] = file.map(_.toDouble) // RDD[Int] of all data

//    val categorized: RDD[(String, Double)] = numbers.map(map1) // RDD[(String, Int)] of three kinds of string: Small, Medium, Large

//    val sumed = categorized.reduceByKey(_+_) //RDD[(String, Int)] of the total of Small, Medium, Large numbers
//    //categorized.collect.foreach(println) val pairdCategorized = new PairRDDFunctions[String, Int](categorized)
//    val counts = categorized.countByKey



    val rangeless100: RDD[Double] = numbers.filter(n => 0 < n && n < 100)
    println(rangeless100.count)
    //val rangeless1000: RDD[Int] = numbers.filter(n => 100 < n && n < 1000)
    //val rangemore1000: RDD[Int] = numbers.filter(n => n > 999)

//    val count1 = rangeless100.count
//    val total1 = rangeless100.reduce(_+_)
//    val mean1 = total1 / count1
//    val devs1: RDD[Long] = rangeless100.map(n => (n - mean1) * (n - mean1))
    //val totalDevs1 = devs1.reduce(_+_)
    //val totalDevs1: RDD[Long] = devs1.coalesce(1)
    //val totalDevs1 = devs1.flatMap()
    //totalDevs1.foreach(println)
    //val valueArray: Array[Long] = totalDevs1.collect()
    //val stddev1 = Math.sqrt(valueArray(0) / count1)

    //println(stddev1)
    //range.collect.foreach(println)

    //val pairs = file.flatMap(line => line.trim().split(" ")).map(word => (word.trim(), 1))

    //val counts = pairs.reduceByKey(_ + _)
    //println(counts.count)
    //println(counts.collect().mkString("\n"))

    lc.setCaptureLineage(false)
    //
    Thread.sleep(1000)
    // Step by step full trace backward
    //val result = counts.testGroundTruth[String, Int]("truth", mapFunc)

    var linRdd = rangeless100.getLineage()
    //linRdd.collect.foreach(println)
            //linRdd.show

    //println(result)
//    for (r <- result) {
//      linRdd = linRdd.filter{ r =>
//        result.contains(r)
//      }
      linRdd = linRdd.goBack()
      linRdd.collect.foreach(println)
   // linRdd = linRdd.goBack()
    linRdd.show
//      linRdd = counts.getLineage()
//      linRdd.collect
//    }



    //        linRdd = linRdd.filter(0)
    //        linRdd.collect
    //        linRdd.show
            //linRdd = linRdd.goBackAll()
            //linRdd.collect.foreach(println)
    //        linRdd.show
    //        linRdd = linRdd.goBack()
    //        linRdd.collect.foreach(println)
    //        linRdd.show
    //
    // Full trace backward
    //        for(i <- 1 to 10) {
    //          var linRdd = counts.getLineage()
    //          linRdd.collect //.foreach(println)
    //              linRdd.show
    // //     linRdd = linRdd.filter(4508) //4508
    //      linRdd = linRdd.goBackAll()
    //      linRdd.collect //.foreach(println)
    //      println("Done")
    //        }
    //    linRdd.show
    //
    //    // Step by step trace backward one record
    //    linRdd = counts.getLineage()
    //    linRdd.collect().foreach(println)
    //    linRdd.show
    //    linRdd = linRdd.filter(262)
    //    linRdd.collect.foreach(println)
    //    linRdd.show
    //    linRdd = linRdd.goBack()
    //    linRdd.collect.foreach(println)
    //    linRdd.show
    //    linRdd = linRdd.goBack()
    //    linRdd.collect.foreach(println)
    //    linRdd.show
    //
    //    // Full trace backward one record
    //    linRdd = counts.getLineage()
    //    linRdd.collect.foreach(println)
    //    linRdd.show
    //    linRdd = linRdd.filter(262)
    //    linRdd.collect.foreach(println)
    //    linRdd.show
    //    linRdd = linRdd.goBackAll()
    //    linRdd.collect.foreach(println)
    //    linRdd.show
    //
    //    // Step by step full trace forward
    //    linRdd = file.getLineage()
    //    linRdd.collect.foreach(println)
    //    linRdd.show
    //    linRdd = linRdd.goNext()
    //    linRdd.collect.foreach(println)
    //    linRdd.show
    //    linRdd = linRdd.goNext()
    //    linRdd.collect.foreach(println)
    //    linRdd.show
    //
    //    // Full trace forward
    //    linRdd = file.getLineage()
    //    linRdd.collect.foreach(println)
    //    linRdd.show
    //    linRdd = linRdd.goNextAll()
    //    linRdd.collect.foreach(println)
    //    linRdd.show
    //
    //    // Step by step trace forward one record
    //    linRdd = file.getLineage()
    //    linRdd.collect().foreach(println)
    //    linRdd.show
    //    linRdd = linRdd.filter(2)
    //    linRdd.collect.foreach(println)
    //    linRdd.show
    //    linRdd = linRdd.goNext()
    //    linRdd.collect.foreach(println)
    //    linRdd.show
    //    linRdd = linRdd.goNext()
    //    linRdd.collect.foreach(println)
    //    linRdd.show

    //    // Full trace forward one record
    //var linRdd = counts.getLineage()
    //    linRdd.collect
    //    linRdd = linRdd.filter(0)
    //    linRdd = linRdd.goBackAll()
    //    linRdd.collect
    //    val value = linRdd.take(1)(0)
    //    println(value)
    ////    sc.unpersistAll(false)
    //    for(i <- 1 to 10) {
    //          var linRdd = file.getLineage().filter(r => (r.asInstanceOf[(Any, Int)] == value))
    //          linRdd.collect()//.foreach(println)
    //      //    linRdd.show
    //          linRdd = linRdd.filter(0)
    //
    //      //    linRdd.collect.foreach(println)
    //      //    linRdd.show
    //          linRdd = linRdd.goNextAll()
    //          linRdd.collect()//.foreach(println)
    //          println("Done")
    //    }
    ////    linRdd.show
    sc.stop()
  }
}

