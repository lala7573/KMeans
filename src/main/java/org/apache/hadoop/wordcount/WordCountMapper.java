package org.apache.hadoop.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

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
public class WordCountMapper
        extends Mapper<LongWritable, Text, Text, IntWritable> {


    private final static IntWritable one = new IntWritable(1);
    private Text word;
    StringTokenizer stringTokenizer;

    @Override
    protected void setup(Context context)  {
        word = new Text();
    }

    protected void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
        stringTokenizer = new StringTokenizer(values.toString());
        while( stringTokenizer.hasMoreTokens() ) {
            word.set(stringTokenizer.nextToken());
            context.write(word, one);
        }

    }
}
