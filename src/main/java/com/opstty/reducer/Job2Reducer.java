package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Job2Reducer extends Reducer<Text, IntWritable, Text, NullWritable>
{
    public void reduce(Text specieskey, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
    {
        context.write(specieskey, NullWritable.get());
    }
}
