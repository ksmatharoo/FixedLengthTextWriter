import com.ksm.SparkUtils;
import com.ksm.hadoop.wordCount.WordCountOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.IOException;
import java.util.Arrays;

public class WordCountFormatTest {

    public static void main(String[] args) throws IOException {

        SparkSession sparkSession = SparkUtils.getSparkSession();
        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
        JavaRDD<String> textFile = jsc.textFile("src/test/resources/input/input.txt");

        JavaPairRDD<String, Integer> pairRDD = textFile
                .flatMap(s -> Arrays.asList(s.split(" ")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);

        pairRDD.saveAsNewAPIHadoopFile("src/test/resources/output/wordCountFile",
                Text.class,
                IntWritable.class,
                WordCountOutputFormat.class
        );
    }
}
