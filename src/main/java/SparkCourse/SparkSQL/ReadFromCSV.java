package SparkCourse.SparkSQL;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;

import static org.apache.spark.sql.functions.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

        //creating View
        data.createOrReplaceTempView("my_students_table");
        Dataset<Row> results = spark.sql("select distinct(year) from my_students_table order by year desc");
        results.show(10);

        //creating in memory data
        List<Row> inmemory = new ArrayList<Row>();

        inmemory.add(RowFactory.create("WARN","16 December 2018"));
        inmemory.add(RowFactory.create("ERROR","17 December 2018"));
        inmemory.add(RowFactory.create("INFO","18 December 2018"));
        inmemory.add(RowFactory.create("DEBUG","19 December 2018"));
        inmemory.add(RowFactory.create("WARN","20 December 2018"));

        StructField[] fields = new StructField[]{
                new StructField("level", DataTypes.StringType,false, Metadata.empty()),
                new StructField("datetime", DataTypes.StringType,false,Metadata.empty())
        };

        StructType schema = new StructType(fields);

        Dataset<Row> dataset = spark.createDataFrame(inmemory,schema);
        dataset.show();



    }
}
