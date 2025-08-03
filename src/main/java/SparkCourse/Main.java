package SparkCourse;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.sources.In;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        System.out.println("Hello world!");
        List<Double> numbers = Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0);
        List<Integer> sqtNum = Arrays.asList(1, 2, 3, 4, 5);

        SparkConf conf = new SparkConf().setAppName("'Spark Application'").setMaster("local[*]");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        JavaRDD<Double> rdd = sparkContext.parallelize(numbers); //creation  of RDD

        JavaRDD<Integer> rddSqt = sparkContext.parallelize(sqtNum);
        Double res= rdd.reduce((a,b)->a + b);
        List<Double> res1= rddSqt.map((a)->Math.sqrt(a)).collect();
        System.out.println("res1: "+ res1);

        JavaRDD<Double> resSqt = rddSqt.map((a)->Math.sqrt(a));
      //  resSqt.foreach(System.out::println); Throws Serialization issue
        resSqt.collect().forEach(System.out::println);
        Long output= resSqt.map(a->1l).reduce((a, b)->a + b);
        System.out.println("o:"+output);

        System.out.println("Sum of numbers: " + res);


        //using Tuple
        JavaRDD<Tuple2<Integer,Double>> sqrtUisngTuple = rddSqt.map(a->new Tuple2<>(a, Math.sqrt(a)));
        sqrtUisngTuple.foreach(a-> System.out.println(a));
        sparkContext.close();
    }
}