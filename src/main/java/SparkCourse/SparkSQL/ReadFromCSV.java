package SparkCourse.SparkSQL;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;
import java.io.IOException;

public class ReadFromCSV {
    public static void main(String[] args) throws IOException {
        SparkSession spark = SparkSession.builder().appName("Spark sql")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> data = spark.read().option("header", true).csv("src/main/resources/students.csv");
        data.show(20);

        Row firstRow = data.first();

        String subject = firstRow.getAs("subject").toString();
        System.out.println(subject);

        int year = Integer.parseInt(firstRow.getAs("year"));
        System.out.println(year);

        Dataset<Row> modernArtResults = data.filter("subject = 'Modern Art' AND year >= 2005");

        Dataset<Row> modernArtResultusingLamda = data.filter((FilterFunction<Row>) r->r.getAs("subject").equals("Modern Art"));
        modernArtResultusingLamda.show();

//        Column subjectColumn = functions.col("subject");
//        Column yearColumn = functions.col("year");

        Dataset<Row> modernArtResultsUsingColumn = data.filter(col("subject").equalTo("Modern Art")
                .and(col("year").geq(2007)));
        modernArtResultsUsingColumn.show();



    }
}
