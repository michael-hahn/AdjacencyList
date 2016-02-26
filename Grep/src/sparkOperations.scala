/**
 * Created by Michael on 1/25/16.
 */

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.MutableList

class sparkOperations extends Serializable {
  def sparkWorks(text: RDD[String], matchTerm: String): RDD[(String, Int)] = {
    val result = text.flatMap(line => {
      val list: MutableList[String] = MutableList()
      val regex = matchTerm.r
      val itr = regex.findAllMatchIn(line)
      while (itr.hasNext) {
        val value = itr.next()
        list += value.toString()
      }
      list.toIterable
    })
      .map(s => (s, 2))//seeded fault, suppose to be 1
      .reduceByKey(_+_)

    result
  }
}
