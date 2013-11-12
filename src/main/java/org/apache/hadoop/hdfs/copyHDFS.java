package org.apache.hadoop.hdfs;


import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;


public class copyHDFS {
    public static void main(String args[]) {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://localhost:9000");

        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
            System.out.println(fs);
            InputStream is = fs.open(new Path("/input/input.csv"));
            //FSDataInputStream os = fs.open(new Path("/output/output.csv"));
            FileUtils.copyInputStreamToFile(is, new File("/Users/YeonjuMac/Desktop/output.csv"));
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.

        }
    }
}