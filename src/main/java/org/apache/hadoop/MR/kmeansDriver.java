package org.apache.hadoop.MR;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



/**
 * Created with IntelliJ IDEA.
 * User: YeonjuMac
 * Date: 13. 9. 4.
 * Time: 오후 9:36
 * To change this template use File | Settings | File Templates.
 */


public class kmeansDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        if(args.length != 2){
            System.err.println("Usage: kmeans <input> <output>");
            System.exit(2);
        }

        Job job = new Job(conf, "kmeans");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setJarByClass(kmeansDriver.class);
        job.setMapperClass(kmeansMapper.class);
        job.setReducerClass(kmeansReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.waitForCompletion(true);
    }
}
