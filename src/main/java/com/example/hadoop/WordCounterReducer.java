package com.example.hadoop;

import lombok.extern.log4j.Log4j;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.MessageFormat;

@Log4j
public class WordCounterReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        IntWritable result = new IntWritable();
        log.info(MessageFormat.format("[reduce] key: {0} start", key));
        int sum = 0;
        for (IntWritable val : values) {
            log.info(MessageFormat.format("[reduce] key: {0}, values: {1}", key, val));
            sum += val.get();
        }
        result.set(sum);
        context.write(key, result);
    }
}
