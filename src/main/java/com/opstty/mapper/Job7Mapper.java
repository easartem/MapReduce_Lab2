package com.opstty.mapper;

import com.opstty.PairWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Job7Mapper extends Mapper<Object, Text, IntWritable, PairWritable> {

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        PairWritable record = new PairWritable();
        IntWritable one = new IntWritable(1);

        IntWritable district = new IntWritable(Integer.parseInt(value.toString().split("\t")[0]));
        IntWritable number = new IntWritable(Integer.parseInt(value.toString().split("\t")[1]));

        record.setFirst(district);
        record.setSecond(number);

        context.write(one, record);
    }
}