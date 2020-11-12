package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Job1Mapper extends Mapper<Object, Text, Text, IntWritable>
{
    // Attribute (key : district number) and (value : 1)
    private Text district = new Text();
    private final static IntWritable one = new IntWritable(1); //NullWritable

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
        if(!value.toString().contains("ARRONDISSEMENT")) // ignore the first line
        {
            district.set(value.toString().split(";")[1]);
            context.write(district, one);
        }
    }
}
