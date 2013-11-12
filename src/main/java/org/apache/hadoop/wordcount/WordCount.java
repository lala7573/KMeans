package org.apache.hadoop.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * [Class description.  The first sentence should be a meaningful summary of the class since it
 * will be displayed as the class summary on the Javadoc package page.]
 * <p/>
 * [Other notes, including guaranteed invariants, usage instructions and/or examples, reminders
 * about desired improvements, etc.]
 *
 * @author <A HREF="mailto:lala7573@gmail.com">Yeonju, Hwang</A>
 * @version 1.0
 * @see class name#method
 * @since 2013. 11. 8.
 */
public class WordCount {

    private static final int JAR_FAIL = -1;
    private static Logger logger = LoggerFactory.getLogger(WordCount.class);

    private static String input;
    private static String output;

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        parseArguments(args);
        if(args.length != 4) {
            logger.error("Usage : WordCount <input> <output> ");
            System.exit(JAR_FAIL);
        }

        TaskCompletionEvent[] taskCompletionEvents = runJob(conf);
        printTaskCompletionEvent(taskCompletionEvents);
    }
    public static void printTaskCompletionEvent(TaskCompletionEvent[] taskCompletionEvents){

        for( TaskCompletionEvent task : taskCompletionEvents){

            logger.info("\ngetEventID : {}", task.getEventId());
            logger.info("getTaskAttemptID : {}", task.getTaskAttemptId());
            logger.info("getTaskStatus : {}", task.getTaskStatus());
            logger.info("getTaskRunTime : {}", task.getTaskRunTime());
            logger.info("isMapTask : {}", task.isMapTask());
            logger.info("isWithinJob : {}", task.idWithinJob());
        }

    }
    public static TaskCompletionEvent[] runJob(Configuration conf) throws IOException, InterruptedException, ClassNotFoundException{
        Job job = new Job(conf);

        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);

        return job.getTaskCompletionEvents(0);
    }
    private static void parseArguments(String[] args){
        logger.info("parseArguments");
        logger.info("args.length : {}", args.length);
        for (int i = 0; i < args.length; ++i) {
            if ( "-input".equals(args[i])) {
                input = args[++i];
            } else if ("-output".equals(args[i])){
                output = args[++i];
            }
        }
    }

}
