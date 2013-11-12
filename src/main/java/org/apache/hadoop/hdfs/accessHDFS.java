package org.apache.hadoop.hdfs;


import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

public class accessHDFS {
    private static Configuration conf;
    private static FileSystem fs;

    public static void main(String args[]) {

        String input = "/input/input.csv" ;
        String local = "/Users/YeonjuMac/Desktop/output.csv";
        try{

            init();

            copyToLocal(input, local);
            cat(input);
            //cat("/input");
            //delete("/input");

        }catch( IOException e){
            e.printStackTrace();
        }
    }

    public static void init(){
        conf = new Configuration();
        conf.set("fs.default.name", "hdfs://localhost:9000");

        try{
            fs = FileSystem.get(conf);
        }catch(IOException e){
            e.printStackTrace();
        }
    }

    public static void delete (String path) throws IOException{
        if( fs.exists(new Path(path) )){
            fs.delete(new Path(path), !isFile(path));
            System.out.println("delete is done : "+path);
        }
    }
    public static boolean isFile(String path)  {
        try{
            if (fs.exists(new Path(path)) ){
                FileStatus f_status = fs.getFileStatus(new Path(path));
                return !f_status.isDir();
            }
        }catch(IOException e){
            System.out.println("\ngiven path is not a file or not exists");
            e.printStackTrace();
        }
        return false;
    }

    public static void cat (String path) throws IOException{
        if( isFile(path) ){
            InputStream is = fs.open(new Path(path));

            IOUtils.copyBytes(is, System.out, 4096, false);
        }
    }

    public static void copyToLocal(String path, String file) throws IOException{

            InputStream is = fs.open(new Path(path));
            FileUtils.copyInputStreamToFile(is, new File(file));

            System.out.println("\ncopy is done");
    }

//    public static void copyToHDFS(String path, String file) throws IOException{
//        if(isFile(path)){
//            InputStream is = fs.open(new Path(path));
//            FileUtils.copyInputStreamToFile(is, fs.open(new Path(file)));
//            System.out.println("\ncopy is done");
//        }
//    }
}