package com.opstty.reducer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

// Sort the trees height from smallest to largest (average)

public class Job5Reducer extends Reducer<FloatWritable, NullWritable, FloatWritable, NullWritable>
{
    public void reduce(FloatWritable heightKey, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
    {
        context.write(heightKey, NullWritable.get());
    }
}