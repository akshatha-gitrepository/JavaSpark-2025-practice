package SparkCourse;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class ReadingFile {

   /**
         * Main method to run the Spark word count application.
         * Reads a text file, splits lines into words, counts occurrences of each word,
         * and prints the word counts to the console.
         *
         * @param args Command line arguments (not used)
         */
        public static void main(String[] args) {
            SparkConf conf = new SparkConf().setAppName("'Spark Application'").setMaster("local[*]");
            JavaSparkContext sc = new JavaSparkContext(conf);

            JavaRDD<String> textRDD = sc.textFile("src/main/resources/text2.file");
          JavaPairRDD<String,Long> reducedText= textRDD.flatMap(line-> Arrays.asList(line.split(" ")).iterator())
                    .mapToPair(a->new Tuple2<String,Long>(a,1L))
                    .reduceByKey((a,b)-> a + b);

            JavaPairRDD<String,Long> sorted = reducedText.sortByKey(false);
            sorted.coalesce(1).foreach(s-> System.out.println(s)); //coalesce

        }
}
