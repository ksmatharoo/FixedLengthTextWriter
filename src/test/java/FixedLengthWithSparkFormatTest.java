import com.google.gson.Gson;
import com.ksm.SparkUtils;
import com.ksm.spark.format.FixedLengthFileFormat;
import com.univocity.parsers.fixed.FixedWidthFields;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

public class FixedLengthWithSparkFormatTest {

    public static void main(String[] args) throws IOException {
        SparkSession sparkSession = SparkUtils.getSparkSession();

        final Dataset<Row> ds = sparkSession.read()
                .option("inferSchema", "true")
                .option("header", "true")
                .csv("src/test/resources/input/Record_5000_withID.csv");

        final String[] fieldNames = ds.schema().fieldNames();
        FixedWidthFields fieldLengths = new FixedWidthFields();
        for (String name : fieldNames) {
            fieldLengths.addField(name, 20);
        }

        //setting up config for FixedLengthFileFormat.class
        Gson gson = new Gson();
        String json = gson.toJson(fieldLengths);
        sparkSession.sparkContext().conf().
                set("fixedLengthFile.fieldSetting", json);

        ds.write().format(FixedLengthFileFormat.class.getName())
                .option("extension", "pat")
                .option("header", "true")
                //.option("compression", "bzip2")
                .mode(SaveMode.Overwrite)
                .save("src/test/resources/output/fixedLenWithSpark");
    }
}