package com.example.hadoophomework;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SortCountReducer extends Reducer<LongWritable, Text, Text, LongWritable> {
    long seq = 1;

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text val : values) {
            Text output = new Text(seq + "\t" + val.toString());
            context.write(output, key);
            seq += 1;
            if (seq >= 100) {
                break;
            }
        }
    }
}
