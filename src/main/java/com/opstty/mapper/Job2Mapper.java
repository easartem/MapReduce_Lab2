package com.opstty.mapper;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import javax.naming.Context;
import java.io.IOException;

public class Job2Mapper extends Mapper<Object, Text, Text, NullWritable>
{
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        if(!value.toString().contains("ESPECE")) // ignore the first line
        {
            Text species = new Text(value.toString().split(";")[3]);
            context.write(species,NullWritable.get());
        }
    }
}
