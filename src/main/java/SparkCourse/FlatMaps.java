package SparkCourse;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FlatMaps {

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

        JavaRDD<String> words = inputErrordata.flatMap(a-> Arrays.asList(a.split(" ")).iterator());
        words.foreach(a-> System.out.println(a)); //flatMap

        //filter
        inputErrordata.flatMap(a->Arrays.asList(a.split(" ")).iterator()).filter(a->a.length()>5)
                .foreach(s-> System.out.println(s));

        JavaRDD<String> textRDD = sc.textFile("src/main/resources/text.file");
        textRDD.flatMap(a->Arrays.asList(a.split(",")).iterator()).filter(a->a.length()>3)
                        .foreach(a-> System.out.println(a));

        JavaRDD<String> textReplaceRDD = sc.textFile("src/main/resources/text.file") //replaceALl
                        .map(a->a.replaceAll("[^a-zA-z\\s]","").toLowerCase())
                                .filter(s->s.length()>0);
        textReplaceRDD.foreach(s-> System.out.println(s));






        sc.close();
    }
}
