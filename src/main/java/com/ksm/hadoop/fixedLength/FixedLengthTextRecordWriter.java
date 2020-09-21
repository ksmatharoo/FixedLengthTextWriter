package com.ksm.hadoop.fixedLength;

import com.univocity.parsers.fixed.FixedWidthFields;
import com.univocity.parsers.fixed.FixedWidthWriter;
import com.univocity.parsers.fixed.FixedWidthWriterSettings;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

public class FixedLengthTextRecordWriter<K, V> extends RecordWriter<K, V> {

    OutputStream out;
    FixedWidthWriter writer;
    StructType dataSchema;

    public FixedLengthTextRecordWriter(OutputStream out,String schemaJson) {
        this.out = out;
        dataSchema = (StructType) StructType.fromJson(schemaJson);

        final String[] fieldNamess = dataSchema.fieldNames();
        FixedWidthFields fieldLengthss = new FixedWidthFields();
        for (String name : fieldNamess) {
            fieldLengthss.addField(name, 20);
        }
        FixedWidthWriterSettings settings = new FixedWidthWriterSettings(fieldLengthss);
        writer = new FixedWidthWriter(this.out, settings);
    }

    private void writeObject(Object o) throws IOException {
        if (o instanceof Row) {
            Row row = (Row)o;
            List<String> columnList = new ArrayList<>();
            StructField[] fields = dataSchema.fields();
            for (int i = 0; i < fields.length; i++) {
                columnList.add(row.get(i).toString());
            }
            writer.writeRow(columnList.toArray(new String[0]));

        }
    }
    public synchronized void write(K key, V value) throws IOException {
        boolean nullKey = key == null || key instanceof NullWritable;
        boolean nullValue = value == null || value instanceof NullWritable;
        if (!nullKey || !nullValue) {
            if (!nullKey) {
                this.writeObject(key);
            }
        }
    }
    public synchronized void close(TaskAttemptContext context) throws IOException {
        this.writer.flush();
        this.writer.close();
        this.out.close();
    }
}
