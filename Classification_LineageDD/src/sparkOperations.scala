/**
 * Created by Michael on 2/1/16.
 */

import java.util.StringTokenizer

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._


import scala.collection.mutable.MutableList
import scala.util.control.Breaks._
import scala.math

class sparkOperations extends Serializable {
  def sparkWorks(text: RDD[String], maxClusters: Int, totalClusters: Int, centroid_ref: Array[Cluster]): RDD[(Int, Iterable[String])] = {
    text.filter(line => {
        val movieIndex = line.indexOf(":")
        if (movieIndex > 0) true
        else false
    })
      //seeded faults:
//      .filter(line => {
//        val movieIndex = line.indexOf(":")
//        val movieIdStr = line.substring(0, movieIndex)
//        if (movieIdStr.equals("1")) false
//        else true
//      })
      .map(line => {
        var movieIdStr = new String()
        var reviewStr = new String()
        var userIdStr = new String()
        var reviews = new String()
        var tok = new String("")

        var clusterId = 0
        val n = new Array[Int](maxClusters)
        val sq_a = new Array[Float](maxClusters)
        val sq_b = new Array[Float](maxClusters)
        val numer = new Array[Float](maxClusters)
        val denom = new Array[Float](maxClusters)

        var max_similarity = 0.0f
        var similarity = 0.0f
        val movie = new Cluster()

        val movieIndex = line.indexOf(":")
        for (r <- 0 until maxClusters) {
          numer(r) = 0.0f
          denom(r) = 0.0f
          sq_a(r) = 0.0f
          sq_b(r) = 0.0f
          n(r) = 0
        }
        //MovieIndex is guaranteed to be larger than 0
        movieIdStr = line.substring(0, movieIndex)
        val movieId = movieIdStr.toLong
        movie.movie_id = movieId
        reviews = line.substring(movieIndex + 1)
        val token = new StringTokenizer(reviews, ",")

        while(token.hasMoreTokens){
          tok = token.nextToken()
          val reviewIndex = tok.indexOf("_")
          userIdStr = tok.substring(0, reviewIndex)
          reviewStr = tok.substring(reviewIndex + 1)
          val userId = userIdStr.toInt
          val review = reviewStr.toInt
          for (r <- 0 until totalClusters) {
            breakable {
              for (q <- 0 until centroid_ref(r).total) {
                val rater = centroid_ref(r).reviews.get(q).rater_id
                val rating = centroid_ref(r).reviews.get(q).rating.toInt
                if (userId == rater) {
                  numer(r) += (review * rating).toFloat
                  sq_a(r) += (review * review).toFloat
                  sq_b(r) += (rating * rating).toFloat
                  n(r) += 1
                  break
                }
              }
            }
          }
        }
        for (p <- 0 until totalClusters) {
          denom(p) = ((Math.sqrt(sq_a(p).toDouble)) * (Math.sqrt(sq_b(p).toDouble))).toFloat
          if (denom(p) > 0) {
            similarity = numer(p) /denom(p)
            if (similarity > max_similarity) {
              max_similarity = similarity
              clusterId = p
            }
          }
        }
        if (movieIdStr.equals("1")) {
        (clusterId + 1, movieIdStr)
        } else {
          (clusterId, movieIdStr)
        }
      })
    .groupByKey()
  }
}
