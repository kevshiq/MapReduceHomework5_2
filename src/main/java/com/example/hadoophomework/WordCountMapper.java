package com.example.hadoophomework;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    private static final Logger logger = LoggerFactory.getLogger(WordCountMapper.class);
    private Set<String> stopWords = new HashSet<>();
    private Text wordTexts = new Text();
    private static final LongWritable one = new LongWritable(1);
    private CSVParser csvParser;

    protected void setup(Context context) throws IOException, InterruptedException {
        csvParser = new CSVParserBuilder()
                .withSeparator(',')
                .withQuoteChar('"')
                .build();

        String stopWordsPath = context.getConfiguration().get("stopwords.path");
        if (stopWordsPath != null) {
            Path path = new Path(stopWordsPath);
            FileSystem fs = FileSystem.get(context.getConfiguration());
            try (FSDataInputStream in = fs.open(path); BufferedReader br = new BufferedReader(new InputStreamReader(in))) {
                String line;
                while ((line = br.readLine()) != null) {
                    stopWords.add(line.trim().toLowerCase());
                }    
            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        try {
            String[] Lines = csvParser.parseLine(line);
            if (Lines.length >= 2) {
                String title = Lines[1].trim();
                if (!title.isEmpty()) {
                    String cleaned = title.replaceAll("[^a-zA-Z0-9\\s]", "").toLowerCase();
                    String[] words = cleaned.split("\\s+");
                    for (String word : words) {
                        if (!word.isEmpty() && !stopWords.contains(word)) {
                            wordTexts.set(word);
                            context.write(wordTexts, one);
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error parsing line: " + line, e);
            context.getCounter("MapperErrors", "ParseErrors").increment(1);
            e.printStackTrace();
        }
    }
}
