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
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FixedLengthOutputWriter extends OutputWriter {

    OutputStream outputStream;
    FixedWidthWriter writer;
    StructType dataSchema;
    TaskAttemptContext context;
    boolean bHeader;
    long count;

    public FixedLengthOutputWriter(String path, StructType dataSchema, TaskAttemptContext context,
                                   String fixedWidthFields, String commaSeparatedFieldLength) {

        this.dataSchema = dataSchema;
        FixedWidthWriterSettings settings = getFixedWidthWriterSettings(fixedWidthFields, commaSeparatedFieldLength);
        this.outputStream = CodecStreams.createOutputStream(context, new Path(path));
        writer = new FixedWidthWriter(outputStream, settings);
        this.context = context;
        bHeader = true;
        count = 0;

    }

    private FixedWidthWriterSettings getFixedWidthWriterSettings(String fixedWidthFields,
                                                                 String commaSeparatedFieldLength) {
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
        //settings.getFormat().setComment('\0');
        return settings;
    }

    private void writeHeaderFooterIfAny() {
        if (Boolean.valueOf(context.getConfiguration().get("header", "false"))
                && bHeader) {
            List<String> columnList = new ArrayList<>();
            Arrays.stream(this.dataSchema.fields()).forEach(field ->
                    columnList.add(field.name())
            );

            DateTimeFormatter formatter = DateTimeFormatter.BASIC_ISO_DATE;
            String formattedDate = formatter.format(LocalDate.now());
            writer.commentRow("Business Date :" + formattedDate);

            writer.writeRow(columnList.toArray(new String[0]));
            bHeader = false;
        }
    }

    @Override
    public void write(InternalRow internalRow) {
        writeHeaderFooterIfAny();

        List<String> columnList = new ArrayList<>();
        StructField[] fields = dataSchema.fields();
        for (int i = 0; i < fields.length; i++) {
            Object obj = internalRow.get(i, fields[i].dataType());
            columnList.add(obj == null ? null : obj.toString());
        }
        writer.writeRow(columnList.toArray(new String[0]));
        count++;
    }

    @Override
    public void close() {
        String footer = String.format("Row Count:%d", count);
        writer.commentRow(footer);
        writer.close();
    }
}
