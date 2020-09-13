package com.ksm.spark.format;

import com.google.gson.Gson;
import com.univocity.parsers.fixed.FixedWidthFields;
import com.univocity.parsers.fixed.FixedWidthWriter;
import com.univocity.parsers.fixed.FixedWidthWriterSettings;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.datasources.CodecStreams;
import org.apache.spark.sql.execution.datasources.OutputWriter;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

public class FixedLengthOutputWriter extends OutputWriter {

    OutputStream outputStream;
    FixedWidthWriter writer;
    StructType dataSchema;

    public FixedLengthOutputWriter(String path, StructType dataSchema, TaskAttemptContext context,
            String fixedWidthFields, String commaSeparatedFieldLength) {

        this.dataSchema = dataSchema;
        FixedWidthFields fixedWidthField;
        if (commaSeparatedFieldLength != null) {
            String[] split = commaSeparatedFieldLength.split(",");
            fixedWidthField = new FixedWidthFields();
            for (String str : split) {
                fixedWidthField.addField(Integer.valueOf(str.trim()));
            }
        } else {
            Gson gson = new Gson();
            fixedWidthField = gson.fromJson(fixedWidthFields, FixedWidthFields.class);
        }
        FixedWidthWriterSettings settings = new FixedWidthWriterSettings(fixedWidthField);
        this.outputStream = CodecStreams.createOutputStream(context, new Path(path));
        writer = new FixedWidthWriter(outputStream, settings);
    }

    @Override
    public void write(InternalRow internalRow) {
        List<String> columnList = new ArrayList<>();
        StructField[] fields = dataSchema.fields();
        for (int i = 0; i < fields.length; i++) {
            columnList.add(internalRow.get(i, fields[i].dataType()).toString());
        }
        writer.writeRow(columnList.toArray(new String[0]));
    }

    @Override
    public void close() {
        writer.close();
    }
}
