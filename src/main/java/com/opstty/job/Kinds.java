package com.opstty.job;

import com.opstty.mapper.KindsCountMapper;
import com.opstty.reducer.KindsCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

public class Kinds {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length < 2) {
            System.err.println("Usage: kinds <in> <out>");
            System.exit(2);
        }

        BasicConfigurator.configure();
        Job job = Job.getInstance(conf, "Kinds"); // Job name
        job.setJarByClass(Kinds.class);
        // Set Mapper to species_mapper
        job.setMapperClass(KindsCountMapper.class);
        // Set Reducer to default_reducer : no particular function to apply
        job.setReducerClass(KindsCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}