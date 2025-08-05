package SparkCourse.Excercise;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.sources.In;
import scala.Tuple2;

public class ViewingFigures {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("'Spark Application'").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<Integer,Integer> textRDD = sc.textFile("src/main/resources/views.txt")
                .mapToPair(line->{
                    String[] cols= line.split(",");
                    return new Tuple2<Integer,Integer>(Integer.valueOf(cols[0]), Integer.valueOf(cols[1]));
                });
        JavaPairRDD<Integer,Integer> chapterRDD = sc.textFile("src/main/resources/chapter.txt")
                .mapToPair(line->{
                    String[] cols= line.split(",");
                    return new Tuple2<Integer,Integer>(Integer.valueOf(cols[0]), Integer.valueOf(cols[1]));
                });

        JavaRDD<Integer> col = textRDD.map(a->a._1);
        col.distinct().foreach(s-> System.out.println(s)); //distinct()

        JavaPairRDD<Integer,Integer> count = textRDD.mapToPair(val->
                new Tuple2<Integer,Integer>(val._2,1)).reduceByKey((a,b)->a+b);  //reduceByKey

        count.foreach(s-> System.out.println(s));


        //Inner join
        JavaPairRDD<Integer,Integer> joinData = textRDD.mapToPair(val->
                new Tuple2<Integer, Integer>(val._2,val._1)).distinct();

        JavaPairRDD<Integer,Tuple2<Integer,Integer>> joinedRDD =joinData.join(chapterRDD);
        //joinedRDD= joinedRDD.distinct();
        joinedRDD.foreach(s-> System.out.println(s));

        sc.close();
    }
}
