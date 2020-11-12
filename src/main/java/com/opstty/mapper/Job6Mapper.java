package com.opstty.mapper;

import com.opstty.PairWritable;
import javafx.util.Pair;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class Job6Mapper extends Mapper<Object, Text, IntWritable, PairWritable>
{
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        PairWritable record = new PairWritable();
        IntWritable one = new IntWritable(1);

        if(!value.toString().contains("ARRONDISSEMENT")) // ignore the first line
        {
            String sdistrict = value.toString().split(";")[1];
            String syear = value.toString().split(";")[5];

            if (!syear.isEmpty())
            {
                IntWritable year = new IntWritable(Integer.parseInt(syear));
                IntWritable district = new IntWritable(Integer.parseInt(sdistrict));
                record.setFirst(year);
                record.setSecond(district);
                context.write(one, record);
            }
        }
    }
}


