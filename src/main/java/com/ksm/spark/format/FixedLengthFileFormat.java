package com.ksm.spark.format;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.util.CompressionCodecs;
import org.apache.spark.sql.execution.datasources.CodecStreams;
import org.apache.spark.sql.execution.datasources.OutputWriter;
import org.apache.spark.sql.execution.datasources.OutputWriterFactory;
import org.apache.spark.sql.execution.datasources.TextBasedFileFormat;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import scala.Option;
import scala.Serializable;
import scala.collection.Seq;
import scala.collection.immutable.Map;
import scala.runtime.AbstractFunction0;

public class FixedLengthFileFormat extends TextBasedFileFormat implements DataSourceRegister , Serializable {

    public static String FIXED_LENGTH_FILE_FIELD_SETTINGS = "fixedLengthFile.fieldSetting";
    public static String COMMA_SEPARATED_FIELD_LENGTH = "comma.separated.field.length";

    @Override
    public Option<StructType> inferSchema(SparkSession sparkSession, Map<String, String> map, Seq<FileStatus> seq) {
        return null;
    }

    @Override
    public OutputWriterFactory prepareWrite(SparkSession sparkSession, Job job, Map<String, String> options,
                                            StructType structType) {

        Configuration conf = job.getConfiguration();
        SparkConf sparkConf = sparkSession.sparkContext().conf();
        String json = sparkConf.get(FIXED_LENGTH_FILE_FIELD_SETTINGS,null);
        String commaSeparated = sparkConf.get(COMMA_SEPARATED_FIELD_LENGTH,
                null);

        Option<String> codec = options.get("compression").orElse(new AbstractFunction0<Option<String>>() {
            @Override
            public Option<String> apply() {
                return options.get("codec");
            }
        });

        if(codec.nonEmpty()){
            String codecClassName = CompressionCodecs.getCodecClassName(codec.get());
            CompressionCodecs.setCodecConfiguration(conf,codecClassName);
        }

        return new OutputWriterFactory() {
            @Override
            public String getFileExtension(TaskAttemptContext context) {
                Option<String> extension = options.get("extension");
                String ext = extension.isEmpty() ? ".txt" : "." + extension.get();
                return  ext + CodecStreams.getCompressionExtension(context);
            }

            @Override
            public OutputWriter newInstance(String path, StructType schema,
                                            TaskAttemptContext context) {
                Configuration configuration = context.getConfiguration();
                if (options.get("headers").nonEmpty()) {
                    configuration.set("header", options.get("headers").get());
                }
                return new FixedLengthOutputWriter(path, schema, context, json, commaSeparated);
            }
        };
    }

    @Override
    public String shortName() {
        return "FixedLengthFileFormat";
    }
}
