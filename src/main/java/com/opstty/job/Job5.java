package com.opstty.job;

import com.opstty.mapper.Job5Mapper;
import com.opstty.reducer.Job5Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

// Sort the trees height from smallest to largest (average)


public class Job5 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: treesSort <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "treesSort");
        job.setJarByClass(com.opstty.job.Job5.class);
        job.setMapperClass(Job5Mapper.class);
        job.setCombinerClass(Job5Reducer.class);
        job.setReducerClass(Job5Reducer.class);
        job.setOutputKeyClass(FloatWritable.class);
        job.setOutputValueClass(NullWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}



