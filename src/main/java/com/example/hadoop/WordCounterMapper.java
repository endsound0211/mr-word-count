package com.example.hadoop;

import lombok.extern.log4j.Log4j;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.StringTokenizer;

@Log4j
public class WordCounterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        log.info(MessageFormat.format("[map] key: {0}, value: {1}", key, value));
        StringTokenizer itr = new StringTokenizer(value.toString());

        while (itr.hasMoreTokens()) {
            Text word = new Text();
            word.set(itr.nextToken());
            context.write(word, new IntWritable(1));
        }
    }
}
