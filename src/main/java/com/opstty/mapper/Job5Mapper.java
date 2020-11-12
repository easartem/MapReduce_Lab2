package com.opstty.mapper;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

// Sort the trees height from smallest to largest (average)

public class Job5Mapper extends Mapper<Object, Text, FloatWritable, NullWritable>
{
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
        if(!value.toString().contains("HAUTEUR")) // ignore the first line
        {
            String treeHeight = value.toString().split(";")[6];
            if (!treeHeight.isEmpty())
            {
                FloatWritable height = new FloatWritable(Float.parseFloat(treeHeight));
                context.write(height,NullWritable.get());
            }
        }
    }
}