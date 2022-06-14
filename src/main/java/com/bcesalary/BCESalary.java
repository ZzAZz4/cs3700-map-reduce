package com.bcesalary;

import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public final class BCESalary {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        Path[] inputPaths = Arrays.stream(otherArgs)
                .limit(otherArgs.length - 1)
                .map(Path::new)
                .toArray(Path[]::new);
        Path outputPath = new Path(otherArgs[otherArgs.length - 1]);
        BCESalary.run(inputPaths, outputPath, conf);
    }

    public static void run(Path[] inputPaths, Path outputPath, Configuration conf) throws Exception {
        Job job = Job.getInstance(conf, "BCE 10 Biggest Salaries");
        FileInputFormat.setInputPaths(job, inputPaths);
        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath, true);

        job.setInputFormatClass(TextInputFormat.class);

        job.setJarByClass(BCESalary.class);
        job.setMapperClass(BCESalaryMapper.class);
        job.setReducerClass(BCESalaryReducer.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(IntWritable.class);

        job.waitForCompletion(true);
    }
}
