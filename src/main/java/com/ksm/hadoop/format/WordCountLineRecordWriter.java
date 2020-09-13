package com.ksm.hadoop.format;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class WordCountLineRecordWriter <K, V> extends RecordWriter<K, V> {

    protected DataOutputStream out;
    private final byte[] recordSeprator;
    private final byte[] fieldSeprator;
    public WordCountLineRecordWriter(DataOutputStream out, String fieldSeprator,String recordSeprator) {
        this.out = out;
        this.fieldSeprator = fieldSeprator.getBytes(StandardCharsets.UTF_8);
        this.recordSeprator = recordSeprator.getBytes(StandardCharsets.UTF_8);
    }
    public WordCountLineRecordWriter(DataOutputStream out) {
        this(out, "\t","\n");
    }
    private void writeObject(Object o) throws IOException {
        if (o instanceof Text) {
            Text to = (Text)o;
            this.out.write(to.getBytes(), 0, to.getLength());
        } else {
            this.out.write(o.toString().getBytes(StandardCharsets.UTF_8));
        }
    }
    public synchronized void write(K key, V value) throws IOException {
        boolean nullKey = key == null || key instanceof NullWritable;
        boolean nullValue = value == null || value instanceof NullWritable;
        if (!nullKey || !nullValue) {
            if (!nullKey) {
                this.writeObject(key);
            }
            if (!nullKey && !nullValue) {
                this.out.write(this.fieldSeprator);
            }
            if (!nullValue) {
                this.writeObject(value);
            }
            this.out.write(recordSeprator);//write custom record separator instead of NEW_LINE
        }
    }
    public synchronized void close(TaskAttemptContext context) throws IOException {
        this.out.close();
    }
}
