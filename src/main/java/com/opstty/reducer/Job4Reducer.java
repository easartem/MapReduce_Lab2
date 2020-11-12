package com.opstty.reducer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

// 4. Maximum height per specie of tree (average)

public class Job4Reducer extends Reducer<Text, FloatWritable, Text, FloatWritable>
{
    private FloatWritable result = new FloatWritable();

    public void reduce(Text speciesKey, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException
    {
        float max = 0;
        for (FloatWritable val : values)
        {
            if (val.get() > max)
            {
                max = val.get();
            }
        }
        result.set(max);
        context.write(speciesKey, result);
    }
}