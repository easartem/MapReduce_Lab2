package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

// 3. Number of trees by species (easy)

public class Job3Mapper extends Mapper<Object, Text, Text, IntWritable>
{
    private final static IntWritable one = new IntWritable(1);

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
        if(!value.toString().contains("ESPECE")) // ignore the first line
        {
            // This mapper returns the name of the species paired with 1 for each row
            Text spName = new Text(value.toString().split(";")[3]);
            context.write(spName,one);
        }
    }
}