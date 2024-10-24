package com.example.hadoophomework;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    private static final Logger logger = LoggerFactory.getLogger(WordCountReducer.class);
    private LongWritable result = new LongWritable(); // Fixed spelling error

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
        long sum = 0;
        
        // Iterate through values and sum them up
        for (LongWritable val : values) {
            sum += val.get();
        }

        // Set the result and write the output
        result.set(sum);
        context.write(key, result);
        
        // Log the result for debugging purposes
        logger.info("Reduced key: {}, Sum: {}", key.toString(), sum);
    }
}
