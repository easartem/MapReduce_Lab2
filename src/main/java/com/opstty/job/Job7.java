package com.opstty.job;

import com.opstty.PairWritable;
import com.opstty.mapper.Job1Mapper;
import com.opstty.mapper.Job7Mapper;
import com.opstty.reducer.Job1Reducer;
import com.opstty.reducer.Job7Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Job7
{
    public static void main(String[] args) throws Exception {

        //-------------------------MapReduce n°1-----------------------------//
        Configuration conf1 = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf1, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: districtWithTrees <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job1 = Job.getInstance(conf1, "treesByDistrict");
        job1.setJarByClass(com.opstty.job.Job7.class); /* GOOD */
        job1.setMapperClass(Job1Mapper.class);
        //job1.setCombinerClass(Job1Reducer.class);
        job1.setReducerClass(Job1Reducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i)
        {
            FileInputFormat.addInputPath(job1, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job1, new Path(otherArgs[otherArgs.length - 1], "outputJob1"));
        job1.waitForCompletion(true);

        //-------------------------MapReduce n°2-----------------------------//

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "maxTrees");
        job2.setJarByClass(com.opstty.job.Job7.class); /* GOOD */
        job2.setMapperClass(Job7Mapper.class);
        //job2.setCombinerClass(Job7Reducer.class);
        job2.setReducerClass(Job7Reducer.class);
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(PairWritable.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(IntWritable.class); // attention à bien tej la boucle for du fileInput usuel
        FileInputFormat.addInputPath(job2, new Path(otherArgs[otherArgs.length - 1], "outputJob1"));
        FileOutputFormat.setOutputPath(job2, new Path(otherArgs[otherArgs.length - 1], "outputJob2"));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
