package org.apache.hadoop.MR;

/**
 * Created with IntelliJ IDEA.
 * User: YeonjuMac
 * Date: 13. 9. 4.
 * Time: 오후 9:37
 * To change this template use File | Settings | File Templates.
 */
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class kmeansReducer extends
        Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
//        int sum = 0;
//        for (IntWritable value : values)
//            sum += value.get();
//        result.set(sum);
        context.write(key, result);
    }

}