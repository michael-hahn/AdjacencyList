/**
 * Created by Michael on 1/25/16.
 */

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

class sparkOperations extends Serializable{
  def sparkWorks(text: RDD[String]): RDD[(String, Int)] = {
    val counts = text.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      //Next map operation is just to seed faults
      .map(pair => {
        if (pair._1.startsWith("I")) {
           (pair._1, pair._2 * 2)
        }
        else (pair._1, pair._2)
      })
      .reduceByKey(_ + _)

    counts
  }
}
