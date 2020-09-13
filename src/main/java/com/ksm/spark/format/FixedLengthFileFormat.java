package com.ksm.spark.format;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.util.CompressionCodecs;
import org.apache.spark.sql.execution.datasources.OutputWriter;
import org.apache.spark.sql.execution.datasources.OutputWriterFactory;
import org.apache.spark.sql.execution.datasources.TextBasedFileFormat;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import scala.Option;
import scala.Serializable;
import scala.collection.Seq;
import scala.collection.immutable.Map;

public class FixedLengthFileFormat extends TextBasedFileFormat implements DataSourceRegister , Serializable {

    public static String FIXED_LENGTH_FILE_FIELD_SETTINGS = "fixedLengthFile.fieldSetting";
    public static String COMMA_SEPARATED_FIELD_LENGTH = "comma.separated.field.length";

    @Override
    public Option<StructType> inferSchema(SparkSession sparkSession, Map<String, String> map, Seq<FileStatus> seq) {
        return null;
    }

    @Override
    public OutputWriterFactory prepareWrite(SparkSession sparkSession, Job job, Map<String, String> map,
                                            StructType structType) {

        Configuration conf = job.getConfiguration();
        String json = conf.get(FIXED_LENGTH_FILE_FIELD_SETTINGS,null);
        String commaSeparated = conf.get(COMMA_SEPARATED_FIELD_LENGTH,
                null);
        CompressionCodecs.setCodecConfiguration(conf, null);

        return new OutputWriterFactory() {
            @Override
            public String getFileExtension(TaskAttemptContext taskAttemptContext) {
                return ".txt";
            }

            @Override
            public OutputWriter newInstance(String path, StructType schema,
                                            TaskAttemptContext context) {
                return new FixedLengthOutputWriter(path, schema, context, json,commaSeparated);
            }
        };
    }

    @Override
    public String shortName() {
        return "FixedLengthFileFormat";
    }
}
