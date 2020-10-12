package com.ksm;

import com.univocity.parsers.fixed.FixedWidthFields;
import com.univocity.parsers.fixed.FixedWidthWriter;
import com.univocity.parsers.fixed.FixedWidthWriterSettings;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ConvertToFixedLength implements FlatMapFunction<Iterator<Row>, String> {
    String[] fieldNames;
    boolean bHeaders;
    long count;
    boolean bLastRow;

    public ConvertToFixedLength(String[] fieldNames) {
        bHeaders = true;
        this.fieldNames = fieldNames;
        count = 0;
        bLastRow = false;
    }

    @Override
    public Iterator<String> call(Iterator<Row> rowIterator) throws Exception {

        FixedWidthFields fieldLengths = new FixedWidthFields();
        for (String name : fieldNames) {
            fieldLengths.addField(name, 20);
        }
        FixedWidthWriterSettings settings = new FixedWidthWriterSettings(fieldLengths);
        FixedWidthWriter writer = new FixedWidthWriter(settings);

        return new Iterator<String>() {
            @Override
            public boolean hasNext() {

                if (!rowIterator.hasNext() && bLastRow == false) {
                    bLastRow = true;
                    return true;
                }
                if (!rowIterator.hasNext()){
                    writer.close();
                }
                return rowIterator.hasNext();
            }

            @Override
            public String next() {
                if (bLastRow) {
                    String footer = String.format("Row Count:%d", count);
                    return writer.commentRowToString(footer);
                }
                StringBuilder stringBuilder = new StringBuilder();
                if (bHeaders) {
                    DateTimeFormatter formatter = DateTimeFormatter.BASIC_ISO_DATE;
                    String formattedDate = formatter.format(LocalDate.now());
                    stringBuilder.append(writer.commentRowToString("Business Date :" + formattedDate));
                    stringBuilder.append("\n");
                    stringBuilder.append(writer.writeRowToString(fieldNames));
                    stringBuilder.append("\n");
                    bHeaders = false;
                }
                Row row = rowIterator.next();
                List<String> columnList = new ArrayList<>();
                for (int i = 0; i < row.length(); i++) {
                    Object obj = row.get(i);
                    String fieldValue = obj == null ? null : obj.toString();
                    columnList.add(fieldValue);
                }
                stringBuilder.append(writer.writeRowToString(columnList.toArray(new String[0])));
                count++;
                return stringBuilder.toString();
            }
        };
    }
}
