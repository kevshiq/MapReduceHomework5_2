package com.example.hadoophomework;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SortCountReducer extends Reducer<LongWritable, Text, Text, LongWritable> {
    private long seq = 1;

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text val : values) {
            Text outputKey = new Text(seq + "\t" + val.toString());
            context.write(outputKey, key);
            seq += 1;
            if (seq >= 100) {
                break;
            }
        }
    }
}
