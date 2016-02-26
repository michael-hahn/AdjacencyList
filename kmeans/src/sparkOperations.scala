import java.util.StringTokenizer

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._


import scala.collection.mutable.MutableList
import scala.util.control.Breaks._
import scala.math

class sparkOperations extends Serializable {
  def sparkWorks(text: RDD[String], maxClusters: Int, totalClusters: Int, centroid_ref: Array[Cluster]): RDD[(Int, String)] = {
    val kmeans_result =
      text.filter(line => {
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
            for (q <- 0 until centroid_ref(r).total) {
              val rater = centroid_ref(r).reviews.get(q).rater_id
              val rating = centroid_ref(r).reviews.get(q).rating.toInt
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
        numMovies += cr.movies.size()
        sumSimilarity += similarity
      }
      if (numMovies > 0) {
        avgSimilarity = sumSimilarity / (numMovies.asInstanceOf[Float])
      }
      diff = 0.0f
      minDiff = 1.0f
      for (s <- 0 until numMovies) {
        diff = (math.abs(avgSimilarity - simArrl(s)))
        //println(s + " has diff: " + diff + " with minDiff: " + minDiff + " with similarity " + simArrl(s) + " and avgSimi " + avgSimilarity)
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
    kmeans_result
  }
}