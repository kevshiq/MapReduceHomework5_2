package com.example.hadoophomework;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortCountMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    private LongWritable stockCount = new LongWritable();
    private Text stockName = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] Lines = value.toString().split("\t");

        String stock = Lines[0].trim();
        if (Lines.length == 2){
            try {
                long cnt = Long.parseLong(Lines[1].trim()); 
                stockCount.set(cnt);
                stockName.set(stock);
                context.write(stockCount, stockName);
            } catch (NumberFormatException e) {
                System.exit(-1);    
            }
        }
    }
}