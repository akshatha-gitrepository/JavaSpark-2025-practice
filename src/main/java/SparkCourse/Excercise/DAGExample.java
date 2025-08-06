package SparkCourse.Excercise;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.codehaus.janino.Java;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class DAGExample {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("'Spark Application'").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> textRDD = sc.textFile("src/main/resources/text.file");
        JavaRDD<String> lettersOnlyRDD = textRDD.map(line->line.replaceAll("[^a-zA-Z]","").toUpperCase());
        JavaRDD<String> removeBlankLines = lettersOnlyRDD.filter(line->line.trim().length() > 0);  //non empty lines are kept after trimming whitespaces
        JavaRDD<String> justWords = removeBlankLines.flatMap(line->Arrays.asList(line.split(",")).iterator());
        JavaPairRDD<String,Long> pairRDD = justWords.mapToPair(word->new Tuple2<String, Long>(word,1L));
        JavaPairRDD<String,Long> totals = pairRDD.reduceByKey((v1,v2)->v1+v2);
        JavaPairRDD<String,Long> sorted = totals.sortByKey(false);
        List<Tuple2<String,Long>> results = sorted.take(5);



       results.forEach(s-> System.out.println(s));
       Scanner scan = new Scanner(System.in);
       scan.nextLine();

       sc.close();

    }
}
