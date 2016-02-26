/**
 * Created by Michael on 1/28/16.
 */

import java.io.{PrintWriter, File}
import java.lang.Exception
import java.util.logging._
import org.apache.spark.{rdd, SparkConf, SparkContext}
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.function.{FlatMapFunction, Function2, PairFunction}
import org.apache.spark.rdd.RDD
import scala.Tuple2
import java.util.{Scanner, Calendar, List, StringTokenizer}

import scala.collection.mutable.MutableList

//remove if not needed
import scala.collection.JavaConversions._

import scala.util.control.Breaks._
import org.apache.spark.lineage.LineageContext
import org.apache.spark.lineage.LineageContext._
import scala.sys.process._

/*
 We need to broadcast the variables that are shared among all the RDDs
*/

object kmeans {
  private val strModelFile = "/Users/Michael/IdeaProjects/Classification/initial_centroids"
  private val maxClusters = 16
  private val centroids = new Array[Cluster](maxClusters)
  private val centroids_ref = new Array[Cluster](maxClusters)

  def mapFunc(str: String): (String, String) = {
    val token = new StringTokenizer(str)
    val key = token.nextToken()
    var value = new String("")
    while (token.hasMoreTokens) {
      value += token.nextToken
      value += " "
    }
    if (value.substring(value.size - 1).equals(" ")) {
      value = value.substring(0, value.size - 1)
    }
    (key, value)
  }

  def initializeCentroids(): Int = {
    var numClust = 0
    for (i <- 0 until maxClusters) {
      centroids(i) = new Cluster()
      centroids_ref(i) = new Cluster()
    }
    val modelFile = new File(strModelFile)
    val opnScanner = new Scanner(modelFile)
    while (opnScanner.hasNext) {
      val k = opnScanner.nextInt()
      centroids_ref(k).similarity = opnScanner.nextFloat()
      centroids_ref(k).movie_id = opnScanner.nextLong()
      centroids_ref(k).total = opnScanner.nextShort()
      val reviews = opnScanner.next()
      val revScanner = new Scanner(reviews).useDelimiter(",")
      while (revScanner.hasNext) {
        val singleRv = revScanner.next()
        val index = singleRv.indexOf("_")
        val reviewer = new String(singleRv.substring(0, index))
        val rating = new String(singleRv.substring(index + 1))
        val rv = new Review()
        rv.rater_id = reviewer.toInt
        rv.rating = rating.toInt.toByte
        centroids_ref(k).reviews.add(rv)
      }
    }
    for (pass <- 1 until maxClusters) {
      for (u <- 0 until (maxClusters - pass)) {
        if (centroids_ref(u).movie_id < centroids_ref(u+1).movie_id) {
          val temp = new Cluster(centroids_ref(u))
          centroids_ref(u) = centroids_ref(u+1)
          centroids_ref(u+1) = temp
        }
      }
    }
    for (l <- 0 until maxClusters) {
      if (centroids_ref(l).movie_id != -1) {
        numClust = numClust + 1
      }
    }
    numClust
  }

//  def main(args:Array[String]): Unit = {
//    val pw = new PrintWriter(new File("/Users/Michael/IdeaProjects/kmeans/lineageResult"))
//
//    val sparkConf = new SparkConf().setMaster("local[8]")
//    sparkConf.setAppName("kmeans_LineageDD-" )
//      .set("spark.executor.memory", "2g")
//
//    val ctx = new SparkContext(sparkConf)
//
//    val lines = ctx.textFile("/Users/Michael/IdeaProjects/Classification/file1_dbug", 1)
//    val totalClusters = initializeCentroids()
//    val constr = new sparkOperations
//    val output = constr.sparkWorks(lines, maxClusters, totalClusters, centroids_ref).collect
//    val itr = output.iterator
//    while (itr.hasNext) {
//      val tupVal = itr.next()
//      pw.append(tupVal._1 + " " + tupVal._2 + "\n")
//    }
//    pw.close()
//    ctx.stop()
//  }

  private val exhaustive = 0

  def main(args: Array[String]): Unit = {
    try {
      val lm: LogManager = LogManager.getLogManager
      val logger: Logger = Logger.getLogger(getClass.getName)
      val fh: FileHandler = new FileHandler("myLog")
      fh.setFormatter(new SimpleFormatter)

      lm.addLogger(logger)
      logger.setLevel(Level.INFO)

      logger.addHandler(fh)

      //      val LineageStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      //      val LineageStartTime = System.nanoTime()
      //      logger.log(Level.INFO, "Record Lineage time starts at " + LineageStartTimestamp)

      val sparkConf = new SparkConf().setMaster("local[8]")

      /*
      //Lineage
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
      //
      */

      sparkConf.setAppName("Kmeans_LineageDD")
        .set("spark.executor.memory", "2g")

      //val ctx = new JavaSparkContext(sparkConf)
      val ctx = new SparkContext(sparkConf)

/*
      //lineage
      val lc = new LineageContext(ctx)
      lc.setCaptureLineage(lineage)
      //
      */

      //Prepare for Hadoop MapReduce
      val clw = new commandLineOperations()
      clw.commandLineWorks()
      //Run Hadoop to have a groundTruth
      Seq("hadoop", "jar", "/Users/Michael/Documents/UCLA Senior/F15/Research-Fall2015/benchmark/examples/kmeans.jar", "org.apache.hadoop.examples.kmeans.Kmeans", "-m", "3", "-r", "1", "/Users/Michael/IdeaProjects/Classification/file1s", "output").!!

/*
      val LineageStartTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      val LineageStartTime = System.nanoTime()
      logger.log(Level.INFO, "Record Lineage time starts at " + LineageStartTimestamp)
*/
      val totalClusters = initializeCentroids()
      /*
      val lines = lc.textFile("/Users/Michael/IdeaProjects/Classification/file1s", 1)

      val kmeans_result =
        lines.filter(line => {
          val movieIndex = line.indexOf(":")
          if (movieIndex > 0) true
          else false
        })
          .map(line => {
            var clusterId = 0
            val n = new Array[Int](maxClusters)
            val sq_a = new Array[Float](maxClusters)
            val sq_b = new Array[Float](maxClusters)
            val numer = new Array[Float](maxClusters)
            val denom = new Array[Float](maxClusters)
            var max_similarity = 0.0f
            var similarity = 0.0f
            val movie = new Cluster()
            val movies_arrl = new ClusterWritable()

            val movieIndex = line.indexOf(":")
            for (r <- 0 until maxClusters) {
              numer(r) = 0.0f
              denom(r) = 0.0f
              sq_a(r) = 0.0f
              sq_b(r) = 0.0f
              n(r) = 0
            }
            //From the following, we assume that movieIndex > 0
            val movieIdStr = line.substring(0, movieIndex)
            val movieId = movieIdStr.toLong
            movie.movie_id = movieId
            val reviews = line.substring(movieIndex + 1)
            val token = new StringTokenizer(reviews, ",")

            while (token.hasMoreTokens) {
              val tok = token.nextToken()
              val reviewIndex = tok.indexOf("_")
              val userIdStr = tok.substring(0, reviewIndex)
              val reviewStr = tok.substring(reviewIndex + 1)
              val userId = userIdStr.toInt
              val review = reviewStr.toInt
              for (r <- 0 until totalClusters) {
                breakable {
                  for (q <- 0 until centroids_ref(r).total) {
                    val rater = centroids_ref(r).reviews.get(q).rater_id
                    val rating = centroids_ref(r).reviews.get(q).rating.toInt
                    if (userId == rater) {
                      numer(r) += review * rating
                      sq_a(r) += review * review
                      sq_b(r) += rating * rating
                      n(r) += 1
                      break
                    }
                  }
                }
              }
            }
            for (p <- 0 until totalClusters) {
              denom(p) = (math.sqrt(sq_a(p).asInstanceOf[Double]) *
                math.sqrt(sq_b(p).asInstanceOf[Double])).asInstanceOf[Float]
              if (denom(p) > 0) {
                similarity = numer(p) / denom(p)
                if (similarity > max_similarity) {
                  max_similarity = similarity
                  clusterId = p
                }
              }
            }
            movies_arrl.movies.add(line)
            movies_arrl.similarities.add(max_similarity)
            movies_arrl.similarity = max_similarity
            (clusterId, movies_arrl)
          })
          .groupByKey
          .flatMap(pair => {
            val values = pair._2.toIterator
            val result: MutableList[(Int, String)] = MutableList()
            var data = new String("")
            var shortline = new String("")
            var sumSimilarity = 0.0f
            var numMovies = 0
            var avgSimilarity = 0.0f
            var similarity = 0.0f
            var s = 0
            var diff = 0.0f
            var minDiff = 1.0f
            var candidate = 0
            val arrl: MutableList[String] = MutableList()
            val simArrl: MutableList[Float] = MutableList()
            var shortLine = new String("")
            while (values.hasNext) {
              val cr = values.next().asInstanceOf[ClusterWritable]
              similarity = cr.similarity
              val arrItr = cr.similarities.listIterator()
              while (arrItr.hasNext) {
                simArrl += arrItr.next()
              }
              for (i <- 0 until cr.movies.size) {
                val oneElm = cr.movies.get(i)
                val indexShort = oneElm.indexOf(",", 1000)
                if (indexShort == -1) {
                  shortLine = new String(oneElm)
                } else {
                  shortLine = new String(oneElm.substring(0, indexShort))
                }
                arrl += shortLine
                result += Tuple2(pair._1, oneElm)
              }
            }
            if (numMovies > 0) {
              avgSimilarity = sumSimilarity / numMovies.asInstanceOf[Float]
            }
            diff = 0.0f
            minDiff = 0.0f
            for (s <- 0 until numMovies) {
              diff = (math.abs(avgSimilarity - simArrl(s))).asInstanceOf[Float]
              if (diff < minDiff) {
                minDiff = diff
                candidate = s
              }
            }
            data = arrl(candidate)
            val index2 = data.indexOf(":")
            val movieStr = data.substring(0, index2)
            val reviews = data.substring(index2 + 1)
            val token = new StringTokenizer(reviews, ",")
            var count = 0
            while (token.hasMoreTokens) {
              token.nextToken()
              count += 1
            }
            val strRes = simArrl(candidate) + " " + movieStr + " " + count + " " + reviews
            result += Tuple2(pair._1, strRes)
            result.toList
          })
      kmeans_result.collect()

      lc.setCaptureLineage(false)
      Thread.sleep(1000)

      val pw = new PrintWriter(new File("/Users/Michael/IdeaProjects/kmeans/lineageResult"))

      val result = kmeans_result.testGroundTruth[String, String]("/Users/Michael/IdeaProjects/kmeans/output/part-00000", mapFunc)
      var linRdd = kmeans_result.getLineage()
      linRdd.collect

      linRdd = linRdd.filter { c => result.contains(c) }
      linRdd = linRdd.goBackAll()
      //At this stage, technically lineage has already find all the faulty data set, we record the time
      val lineageEndTime = System.nanoTime()
      val lineageEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      logger.log(Level.INFO, "Lineage takes " + (lineageEndTime - LineageStartTime) / 1000 + " microseconds")
      logger.log(Level.INFO, "Lineage ends at " + lineageEndTimestamp)


      linRdd.show.collect().foreach(s => {
        pw.append(s.toString)
        pw.append('\n')
      })

      pw.close()
*/
      //val lineageResult = ctx.textFile("/Users/Michael/IdeaProjects/kmeans/lineageResult", 1)
      val lineageResult = ctx.textFile("/Users/Michael/IdeaProjects/Classification/file1s", 1)

      val num = lineageResult.count()
      logger.log(Level.INFO, "Lineage caught " + num + " records to run delta-debugging")


      //Remove output before delta-debugging
      val outputFile = new File("/Users/Michael/IdeaProjects/kmeans/output")
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
          new Split, maxClusters, totalClusters, centroids_ref, lm, fh)
      } else {
        val delta_debug: DD_NonEx[String] = new DD_NonEx[String]
        delta_debug.ddgen(lineageResult, new Test, new Split, maxClusters, totalClusters, centroids_ref, lm, fh)
      }

      val DeltaDebuggingEndTime = System.nanoTime()
      val DeltaDebuggingEndTimestamp = new java.sql.Timestamp(Calendar.getInstance.getTime.getTime)
      logger.log(Level.INFO, "DeltaDebugging (unadjusted) ends at " + DeltaDebuggingEndTimestamp)
      logger.log(Level.INFO, "DeltaDebugging (unadjusted) takes " + (DeltaDebuggingEndTime - DeltaDebuggingStartTime) / 1000 + " microseconds")


      //To print out the result
      //    for (tuple <- output) {
      //      println(tuple._1 + ": " + tuple._2)
      //    }
      println("Job's DONE!")
      ctx.stop()
    }
  }
}
