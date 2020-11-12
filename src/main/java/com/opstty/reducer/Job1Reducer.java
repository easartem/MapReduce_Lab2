package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Job1Reducer extends Reducer<Text, IntWritable, Text, IntWritable>
{
    public void reduce(Text districtKey, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
    {
        int sum = 0;
        for (IntWritable val : values)
        {
            sum += val.get();
        }
        IntWritable nbTrees = new IntWritable(sum);
        context.write(districtKey, nbTrees);
    }
}