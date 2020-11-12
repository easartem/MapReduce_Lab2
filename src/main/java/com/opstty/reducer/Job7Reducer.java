package com.opstty.reducer;

import com.opstty.PairWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class Job7Reducer extends Reducer<IntWritable, PairWritable, IntWritable, IntWritable>
{
    public void reduce(IntWritable oneForAll, Iterable<PairWritable> values, Context context) throws IOException, InterruptedException
    {
        int maxTrees = 0;
        int dopeDistrict = 0;

        for (PairWritable val : values)
        {
            if (maxTrees < val.getSecond().get())
            {
                dopeDistrict = val.getFirst().get();
                maxTrees = val.getSecond().get();
            }
        }
        IntWritable winner = new IntWritable(dopeDistrict);
        IntWritable max = new IntWritable(maxTrees);
        context.write(winner, max);
    }
}