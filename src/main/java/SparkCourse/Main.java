package SparkCourse;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        System.out.println("Hello world!");
        List<Double> numbers = Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0);

        SparkConf conf = new SparkConf().setAppName("'Spark Application'").setMaster("local[*]");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        JavaRDD<Double> rdd = sparkContext.parallelize(numbers);
        Double res= rdd.reduce((a,b)->a + b);
        System.out.println("Sum of numbers: " + res);
        sparkContext.close();

    }
}