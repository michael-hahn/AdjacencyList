/**
 * Created by Michael on 11/12/15.
 */
import org.apache.spark.api.java.JavaRDD
import java.util.List
//remove if not needed
import scala.collection.JavaConversions._

trait userSplit[T] {

  def usrSplit(inputList: JavaRDD[T], splitTimes: Int): Array[JavaRDD[T]]
}
