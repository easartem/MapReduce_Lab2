package com.opstty.reducer;

import com.opstty.PairWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;


public class Job6Reducer extends Reducer<IntWritable, PairWritable, IntWritable, NullWritable>
{
    public void reduce(IntWritable oneForAll, Iterable<PairWritable> values, Context context) throws IOException, InterruptedException
    {
        int minYear = 2020;
        int district = 0;
        for (PairWritable val : values)
        {
            if (minYear > val.getFirst().get())
            {
                minYear = val.getFirst().get();
                district = val.getSecond().get();
            }
        }
        IntWritable oldestTreeDS = new IntWritable(district);
        context.write(oldestTreeDS, NullWritable.get());
    }
}