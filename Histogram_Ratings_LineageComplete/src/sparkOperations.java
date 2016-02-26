import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Created by Michael on 11/21/15.
 */
public class sparkOperations implements Serializable {
    JavaPairRDD<Integer, Integer> sparkWorks(JavaRDD<String> text) {
        JavaRDD<Integer> ratings = text.flatMap(new FlatMapFunction<String, Integer>() {
            @Override
            public Iterable<Integer> call(String s) throws Exception {
                List<Integer> intList = new ArrayList<Integer>();
                int rating, reviewIndex, movieIndex, movieID;
                Long rater;
                String reviews = new String();
                String tok = new String();
                String ratingStr = new String();
                String raterStr = new String();
                movieIndex = s.indexOf(":");
                if (movieIndex > 0) {
                    movieID = Integer.parseInt(s.substring(0, movieIndex));
                    reviews = s.substring(movieIndex + 1);
                    StringTokenizer token = new StringTokenizer(reviews, ",");
                    while (token.hasMoreTokens()) {
                        tok = token.nextToken();
                        reviewIndex = tok.indexOf("_");
                        if (reviewIndex > 0) {
                            raterStr = tok.substring(0, reviewIndex);
                            ratingStr = tok.substring(reviewIndex + 1);
                            rating = Integer.parseInt(ratingStr);
                            rater = Long.parseLong(raterStr);
                            //Seed fault
                            //No-fault version: no if statement; only one statement: intList.add(rating)
                            //10 faulty records
                            if (movieID % 13 == 0) {
                                if (rating == 1 && rater % 13 == 0 && rater % 7 == 0 && rater % 24 == 0 && rater % 16 == 0) {
                                    //do nothing
                                } else intList.add(rating);
                            } else intList.add(rating);
                        }
                    }
                }
                return intList;
            }
        });

        JavaPairRDD<Integer, Integer> ones = ratings.mapToPair(new PairFunction<Integer, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Integer integer) throws Exception {
                return new Tuple2<Integer, Integer>(integer, 1);
            }
        });

        JavaPairRDD<Integer, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        return counts;
    }
}
