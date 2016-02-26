/**
 * Created by Michael on 11/12/15.
 */
import org.apache.spark.api.java.JavaRDD
import java.util.List

import org.apache.spark.lineage.rdd.ShowRDD
import org.apache.spark.rdd.RDD

//remove if not needed
import scala.collection.JavaConversions._

trait userSplit[T] {

  def usrSplit(inputList: RDD[T], splitTimes: Int): Array[RDD[T]]
}
