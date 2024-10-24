package com.example.hadoophomework;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountDriver {
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        conf.set("stopwords.path", args[1]);
        Job job1 = Job.getInstance(conf, "Word count");
        job1.setJarByClass(WordCountDriver.class);
        job1.setMapperClass(WordCountMapper.class);
        job1.setReducerClass(WordCountReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));
        if (!job1.waitForCompletion(true)) {
            System.exit(-1);
        }

        Job job2 = Job.getInstance(conf, "Sort");
        job2.setJarByClass(WordCountDriver.class);
        job2.setMapperClass(SortCountMapper.class);
        job2.setReducerClass(SortCountReducer.class);
        job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);
        job2.setOutputKeyClass(LongWritable.class);
        job2.setOutputValueClass(Text.class);
        job2.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job2, new Path(args[2]));
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));
        if (!job2.waitForCompletion(true)) {
            System.exit(-1);
        }
    }
}