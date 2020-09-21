import com.ksm.SparkUtils;
import com.ksm.hadoop.fixedLength.FixedLengthTextOutputFormat;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

public class FixedLengthTextOutputFormatTest {
    public static void main(String[] args) throws IOException {
        SparkSession sparkSession = SparkUtils.getSparkSession();

        final Dataset<Row> ds = sparkSession.read()
                .option("inferSchema", "true")
                .option("header", "true")
                .csv("src/test/resources/input/Record_5000_withID.csv");

        sparkSession.sparkContext().hadoopConfiguration().
                set(FixedLengthTextOutputFormat.SCHEMA_JSON,ds.schema().json());

        ds.javaRDD().zipWithIndex().saveAsNewAPIHadoopFile(
                "src/test/resources/output/fixedLenWithHadoop",
                Row.class,
                Void.class,
                FixedLengthTextOutputFormat.class
        );
    }
}
