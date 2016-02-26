/**
 * Created by Michael on 11/12/15.
 */

import java.util.logging.{FileHandler, LogManager}

import org.apache.spark.api.java.JavaRDD
//remove if not needed
import scala.collection.JavaConversions._

trait userTest[T] {

  def usrTest(inputRDD: JavaRDD[T], maxClusters: Int, totalClusters: Int, centroid_ref: Array[Cluster], lm: LogManager, fh: FileHandler): Boolean
}
