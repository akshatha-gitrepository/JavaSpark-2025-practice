package SparkCourse;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class RddExample {

    public static void main(String[] args) {

        List<String> errorData = new ArrayList<>();
        errorData.add("WARN: Tuesday 4 September 0405");
        errorData.add("ERROR: Tuesday 4 September 0405");
        errorData.add("INFO: Tuesday 4 September 0405");
        errorData.add("ERROR: Wednesday 5 September 0505");
        errorData.add("WARN: Wednesday 5 September 0505");

        SparkConf conf = new SparkConf().setAppName("'Spark Application'").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> inputErrordata = sc.parallelize(errorData); //JavaRDD

        JavaPairRDD<String, String > data =inputErrordata.mapToPair(a->{ //creating JavaPairRDD
            String[] values= a.split(":");
            String level = values[0];
            String date= values[1];
                    return new Tuple2<>(level,date);
                }
                );

        JavaPairRDD<String,Iterable<String>> groupByData = data.groupByKey();// GroupByKey example

        List<Tuple2<String, Iterable<String>>> elements = groupByData.collect();
        for(Tuple2<String, Iterable<String>> elem : elements){
            System.out.println(elem._1 +":");
            for (String ele : elem._2){
                System.out.println(ele + "|");
            }
            System.out.println();
        }
        sc.close();


    }
}
