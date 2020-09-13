package com.ksm;

import com.univocity.parsers.fixed.FixedWidthFields;
import com.univocity.parsers.fixed.FixedWidthWriter;
import com.univocity.parsers.fixed.FixedWidthWriterSettings;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ConvertToFixedLength implements FlatMapFunction<Iterator<Row>, String> {
    String[] fieldNames;

    public ConvertToFixedLength(String[] fieldNames ) {
        this.fieldNames = fieldNames;
    }

    @Override
    public Iterator<String> call(Iterator<Row> rowIterator) throws Exception {

        FixedWidthFields fieldLengths = new FixedWidthFields();
        for (String name : fieldNames) {
            fieldLengths.addField(name, 20);
        }
        FixedWidthWriterSettings settings = new FixedWidthWriterSettings(fieldLengths);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        FixedWidthWriter writer = new FixedWidthWriter(out, settings);

        return new Iterator<String>() {
            @Override
            public boolean hasNext() {
                out.reset();
                return rowIterator.hasNext();
            }

            @Override
            public String next() {
                Row row = rowIterator.next();
                List<String> columnList = new ArrayList<>();
                for (int i = 0; i < row.length(); i++) {
                    String fieldValue = row.get(i).toString();
                    columnList.add(fieldValue);
                }
                writer.writeRow(columnList.toArray(new String[0]));
                writer.flush();
                return new String(out.toByteArray(), 0, out.toByteArray().length - 1);
            }
        };
    }
}
