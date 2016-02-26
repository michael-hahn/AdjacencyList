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


import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._
import org.apache.spark.{SparkConf, SparkContext}


object SparkWordCount {
  def main(args: Array[String]) {
    val conf = new SparkConf()
    var lineage = true
    var logFile = "/Users/Michael/Desktop/spark-lineage-1.2.1/README.md"
    if(args.size < 2) {
      //logFile = "biomed"
      conf.setMaster("local[2]")
      lineage = true
    } else {
      lineage = args(0).toBoolean
//      logFile += args(1)
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
    conf.setAppName("WordCount-" + lineage + "-" + logFile)

    val sc = new SparkContext(conf)
    val lc = new LineageContext(sc)

    lc.setCaptureLineage(lineage)

    // Job
    val file = lc.textFile(logFile, 2)
    val pairs = file.flatMap(line => line.trim().split(" ")).map(word => (word.trim(), 1))
    val counts = pairs.reduceByKey(_ + _)
    counts.count
//    for (c <- counts) {
//      println(c._1 + " : " + c._2)
//    }
    //    println(counts.collect().mkString("\n"))

    lc.setCaptureLineage(false)
    //
    Thread.sleep(1000)
    // Step by step full trace backward
        var linRdd = counts.getLineage()
        linRdd.collect.foreach(println)
        linRdd = linRdd.filter(100)
        linRdd.show
        linRdd = linRdd.goBack()
        linRdd.collect.foreach(println)
        linRdd.show
        linRdd = linRdd.goBack()
        linRdd.collect.foreach(println)
        linRdd.show


    // Full trace backward
    //    for(i <- 1 to 10) {
    //      var linRdd = counts.getLineage()
    //      linRdd.collect //.foreach(println)
    //      //    linRdd.show
    // //     linRdd = linRdd.filter(4508) //4508
    //      linRdd = linRdd.goBackAll()
    //      linRdd.collect //.foreach(println)
    //      println("Done")
    //    }
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
//        var linRdd = file.getLineage()
//        linRdd.collect.foreach(println)
//        println("Go forward Result: ***********")
//        linRdd.show
//        linRdd = linRdd.goNext()
//        linRdd.collect.foreach(println)
//        println("Go forward once Result: **********")
//        linRdd.show
//        linRdd = linRdd.goNext()
//        linRdd.collect.foreach(println)
//        println("Go forward twice times Result: ****")
//        linRdd.show
    //
    //    // Full trace forward
    //    linRdd = file.getLineage()
    //    linRdd.collect.foreach(println)
    //    linRdd.show
    //    linRdd = linRdd.goNextAll()
    //    linRdd.collect.foreach(println)
    //    linRdd.show
    //
        // Step by step trace forward one record
        linRdd = counts.getLineage().goBackAll()
        linRdd.collect().foreach(println)
        linRdd.show
        linRdd = linRdd.filter(2)
        linRdd.collect.foreach(println)
        linRdd.show
        linRdd = linRdd.goNext()
        linRdd.collect.foreach(println)
        linRdd.show
        linRdd = linRdd.goNext()
        linRdd.collect.foreach(println)
        linRdd.show

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
