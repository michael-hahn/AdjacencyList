import org.apache.commons.logging.LogFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.*;

/**
 * Created by Michael on 11/5/15.
 */
public final class AdjacencyList {

    public static final int LIMIT = 200000;

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setAppName("AdjacencyList").setMaster("local[1]");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        java.sql.Timestamp startTimestamp = new java.sql.Timestamp(Calendar.getInstance().getTime().getTime());
        long startTime = System.nanoTime();
        JavaRDD<String> lines = ctx.textFile("edges_31", 1);

        //Date startTime = new Date();
        //System.out.println("Job Started: " + startTime);


        JavaPairRDD<String, String> edgesList = lines.flatMapToPair(new PairFlatMapFunction<String, String, String>() {
            @Override
            public Iterable<Tuple2<String, String>> call(String s) throws Exception {
                List<Tuple2<String, String>> listofEdges = new ArrayList<Tuple2<String, String>>();
                int index = s.lastIndexOf(",");
                if (index == -1) {
                    System.out.println("Input in Wrong Format: " + s);
                }
                String outEdge = s.substring(0, index);
                String inEdge = s.substring(index + 1);
                String outList = "from{" + outEdge + "}:to{}";
                String inList = "from{}:to{" + inEdge + "}";
                Tuple2<String, String> out = new Tuple2<String, String>(outEdge, inList);
                Tuple2<String, String> in = new Tuple2<String, String>(inEdge, outList);
                listofEdges.add(out);
                listofEdges.add(in);
                return listofEdges;
            }
        });

        /* To see the result of the flapMapToPair function
        List<Tuple2<String, String>> output = edgesList.collect();
        for (Tuple2<?,?> tuple: output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
        */

        JavaPairRDD<String, Iterable<String>> groupedEdgeList = edgesList.groupByKey();

        JavaPairRDD<String, String> result = groupedEdgeList.mapValues(new Function<Iterable<String>, String>() {
            @Override
            public String call(Iterable<String> strings) throws Exception {
                List<String> fromList = new ArrayList<String>();
                List<String> toList = new ArrayList<String>();
                String str = new String();
                String fromLine = new String();
                String toLine = new String();
                String vertex = new String();
                int r, strLength, index;
                Iterator<String> itr = strings.iterator();
                while(itr.hasNext()) {
                    str = itr.next();
                    strLength = str.length();
                    index = str.indexOf(":");
                    if (index == -1) {
                        System.out.println("Wrong Input: " + str);
                        continue;
                    }
                    if(index > 6) // non-empty fromList
                        fromLine = str.substring(5,index-1);
                    if(index + 5 < strLength) // non-empty toList
                        toLine  = str.substring(index+4, strLength-1);

                    if(!fromLine.isEmpty()){
                        StringTokenizer itr2 = new StringTokenizer(fromLine,",");
                        while(itr2.hasMoreTokens()) {
                            vertex = new String(itr2.nextToken());
                            if(!fromList.contains(vertex) && fromList.size() < LIMIT) //avoid heap overflow
                                fromList.add(vertex);
                        }
                    }
                    if(!toLine.isEmpty()) {
                        StringTokenizer itr2 = new StringTokenizer(toLine, ",");
                        while (itr2.hasMoreTokens()) {
                            vertex = new String(itr2.nextToken());
                            if (!toList.contains(vertex) && toList.size() < LIMIT) // avoid heap overflow
                                toList.add(vertex);
                        }
                    }
                }
                Collections.sort(fromList);
                Collections.sort(toList);
                String fromList_str = new String("");
                String toList_str = new String("");
                for (r = 0; r < fromList.size(); r++)
                    if(fromList_str.equals(""))
                        fromList_str = fromList.get(r);
                    else
                        fromList_str = fromList_str + "," + fromList.get(r);
                for (r = 0; r < toList.size(); r++)
                    if(toList_str.equals(""))
                        toList_str = toList.get(r);
                    else
                        toList_str = toList_str + "," + toList.get(r);

                String outValue = new String("from{" + fromList_str + "}:to{" + toList_str + "}");
                return outValue;
            }
        });

        long endTime = System.nanoTime();
        java.sql.Timestamp endTimestamp = new java.sql.Timestamp(Calendar.getInstance().getTime().getTime());

        System.out.println("This job started at " + startTimestamp);
        System.out.println("This job finished at: " + endTimestamp);
        System.out.println("The job took: " + (endTime - startTime)/1000000 + " milliseconds to finish");

//        Date end_time = new Date();
//        System.out.println("Job ended: " + end_time);
//
//        System.out.println("The job took " + (end_time.getTime() - startTime.getTime()) / 1000 + " seconds.");

        /* To see the result of the function
        List<Tuple2<String, String>> output = result.collect();
        for (Tuple2<?,?> tuple: output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
        */

        ctx.stop();
    }
}
