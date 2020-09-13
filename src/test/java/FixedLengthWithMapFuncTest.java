import com.ksm.ConvertToFixedLength;
import com.ksm.SparkUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

public class FixedLengthWithMapFuncTest {
    public static void main(String[] args) throws IOException {
        SparkSession sparkSession = SparkUtils.getSparkSession();
        final Dataset<Row> ds = sparkSession.read()
                .option("inferSchema", "true")
                .option("header", "true")
                .csv("src/test/resources/input/Record_5000_withID.csv");

        final String[] fieldNames = ds.schema().fieldNames();

        final JavaRDD<String> stringJavaRDD = ds.javaRDD().
                mapPartitions(new ConvertToFixedLength(fieldNames));
        stringJavaRDD.saveAsTextFile("src/test/resources/output/fixedLenWithMapFunc");
    }
}
