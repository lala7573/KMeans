package doit.hadoop.wordcount;

import doit.hadoop.wordcount.WordCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;


/**
 * Created with IntelliJ IDEA.
 * User: YeonjuMac
 * Date: 13. 9. 9.
 * Time: 오후 8:15
 * To change this template use File | Settings | File Templates.
 * WordCount Example by Do it hadoop
 */
public class findString {
    private static Configuration conf;
    private static Job job;

    public static class MyMapper
            extends Mapper<LongWritable, Text, Text, LongWritable>{
        private final static LongWritable one = new LongWritable (1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException{
            String line = value.toString().toLowerCase();
            if(line.contains(job.getConfiguration().getStrings("str")[0])){
                word.set(line);
                context.write(word,one);
            }
        }
    }

    public static class MyReducer
            extends Reducer<Text, LongWritable, Text, LongWritable>{
        private LongWritable sumWritable = new LongWritable();

        public void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException{
            long sum = 0;
            for(LongWritable val : values){
                sum += val.get();
            }
            sumWritable.set(sum);
            context.write(key, sumWritable);
        }
    }

    public static void main(String[] args) throws Exception {
        conf = new Configuration();
        job = new Job(conf, "findString");

        job.setJarByClass(WordCount.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        args[2].toLowerCase();
        job.getConfiguration().setStrings("str", args[2]);

        job.waitForCompletion(true);
    }

}
