/**
 * Created by Michael on 11/12/15.
 */
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.rdd.RDD

//remove if not needed
import scala.collection.JavaConversions._

class Split extends userSplit[String] {

  def usrSplit(inputList: RDD[String], splitTimes: Int): Array[RDD[String]] = {
    val weights = Array.ofDim[Double](splitTimes)
    for (i <- 0 until splitTimes) {
      weights(i) = 1.0 / splitTimes.toDouble
    }
    val rddList = inputList.randomSplit(weights)
    rddList
  }
}
