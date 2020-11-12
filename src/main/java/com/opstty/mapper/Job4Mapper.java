package com.opstty.mapper;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

// 4. Maximum height per specie of tree (average)

public class Job4Mapper extends Mapper<Object, Text, Text, FloatWritable>
{
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException
    {
        if(!value.toString().contains("ESPECE")) // ignore the first line
        {
            Text spName = new Text(value.toString().split(";")[3]);
            String treeHeight = value.toString().split(";")[6];
            if (!treeHeight.isEmpty())
            {
                FloatWritable height = new FloatWritable(Float.parseFloat(treeHeight));
                context.write(spName,height);
            }
        }
    }
}