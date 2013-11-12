package org.apache.hadoop.MR;

import org.apache.hadoop.KMeans.Cluster;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Vector;

/**
 * Created with IntelliJ IDEA.
 * User: YeonjuMac
 * Date: 13. 9. 4.
 * Time: 오후 9:36
 * To change this template use File | Settings | File Templates.
 */

public class kmeansMapper extends
        Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable outputValue = new IntWritable(1);
    private static Text wordText = new Text();

    private int k ;
    private Cluster[] clusteringSet;
    private double x;
    private double y;
    private IntWritable integer ;

    private int findNearestCentroidIndex(Vector<Double> c){
        double min = 0;
        int store_number = 0;
        for(int i = 0 ; i < k ; i ++){
            double distance = 0;
            //double distance = clusteringSet[i].centroid.getDistance(c);
            if(min > distance){
                min = distance;
                store_number = i;
            }
        }
        return store_number;
    }
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String column = value.toString();
        String[] columns = value.toString().split(",");
        x = Double.parseDouble(columns[0]);
        y = Double.parseDouble(columns[1]);
        //Coordinate point = new Coordinate(x, y);
        Vector<Double> point = new Vector<Double> ();
        point.set(1, x);
        point.set(2, y);

        IntWritable integer = new IntWritable( findNearestCentroidIndex(point));
        if (columns != null && columns.length > 0) {
            try {
                wordText.set(column);
                context.write(wordText, integer);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}