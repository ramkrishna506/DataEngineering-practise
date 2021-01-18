import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Javacontext {


    public static void main(String[] args){

        SparkConf sparkConf = new SparkConf().setAppName("sampple").setMaster("local");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        /*List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);

        JavaRDD<Integer> distData = javaSparkContext.parallelize(data);
        distData.collect().forEach(System.out::println);*/

        JavaRDD<String> lines = javaSparkContext.textFile("C:/Users/Ramakrishna/Desktop/testing.txt");
        /*JavaRDD<Integer> lineLengths = lines.map(s -> s.length());
        int totalLength = lineLengths.reduce((a, b) -> a + b);*/
        /*System.out.println("****"+totalLength);*/

        JavaPairRDD<String, Integer> pairs = lines.mapToPair(s -> new Tuple2(s, 1));
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b);

        for (Tuple2 String:counts.collect()) {
            System.out.println("key"+String._1+"value"+String._2);

        }


    }


}
