/*
 * 2013 10 18 hwang Yeonju
 * K Means Clustering
 */
package org.apache.hadoop.KMeans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Vector;


/**
 * k means
 */
public class KMeansDriver extends org.apache.hadoop.conf.Configured implements org.apache.hadoop.util.Tool {

    private static final Logger logger = LoggerFactory.getLogger(KMeansDriver.class);
    private String input = null;
    private String cluster = null;
    private String output = null;
    private int iteration = 0;
    private int K = 1;

    public final static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new KMeansDriver(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {

        Configuration conf = new Configuration();
        parseArguments(args, conf);

        //먼저 k개의 값을 가져와서 centroid_0 를 만들자!
        setInitialCentroid(conf);

        //클러스터링을 하고 새로 centroid를 계산한다.

        int i = 0 ;
        do {
            increaseIteration(conf);
            System.out.println("iterator : " + iteration);
            if (!runIteration(conf)) {
                logger.error("runIteration is broken : false");
            }
            ++i;
        } while (false);//isCentroidEqual(conf) || i < 5);//isCentroidEqual(conf));     //여기에서 iterator를 증가시킴


        logger.info("do RunJob : ") ;
        logger.info("iterator : "+iteration);
        runJob(conf);
        logger.info("RunJob done") ;
        return (true) ? 0 : 1;
    }

    public boolean isCentroidEqual(Configuration conf)
            throws IOException {

        Path oldPath = new Path(getIterationPath(iteration - 1)+"/part-r-00000");
        Path newPath = new Path(getIterationPath(iteration)+"/part-r-00000");

        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream oldFos = fs.open(oldPath);
        FSDataInputStream newFos = fs.open(newPath);
        BufferedReader oldBr = new BufferedReader(new InputStreamReader(oldFos));
        BufferedReader newBr = new BufferedReader(new InputStreamReader(newFos));

        int i;
        for (i = 0; i < K; i++) {
            String line1 = oldBr.readLine();
            String line2 = newBr.readLine();
            if (!line1.equals(line2)) {
                logger.info(line1 + " and " + line2);
                break;
            }
        }

        oldBr.close();
        newBr.close();
        oldFos.close();
        newFos.close();
        fs.close();

        return i == K ? true : false;
    }

    private void increaseIteration(Configuration conf) {
        ++iteration;
        conf.set(Constants.CLUSTER_ITERATOR, (iteration) + "");
    }

    private String getIterationPath(int iter) {
        return cluster + "/centroid_" + (iter);
    }

    public boolean setInitialCentroid(Configuration conf) throws IOException {
        final FileSystem fs = FileSystem.get(conf);

        Path inputPath = new Path(input);
        Path clusterPath = new Path(cluster);
        Path iterationPath = new Path(getIterationPath(iteration)+"/part-r-00000");

        //파일인지 아닌지 체크하고 파일인걸 찾음
        if (!fs.isFile(inputPath)) {
            boolean isFile = false;
            while (!isFile) {
                FileStatus[] status = fs.listStatus(inputPath, new PathFilter() {
                    @Override
                    public boolean accept(Path path) {
                        try {
                            return fs.isFile(path);
                        } catch (IOException e) {
                            return false;
                        }
                    }
                });

                //logger로 읽는 파일 출력
                for (FileStatus fileStatus : status) {
                    logger.info(fileStatus.getPath().toString());
                }

                //출력끝
                if (fs.isFile(status[0].getPath())) isFile = true;

                inputPath = status[0].getPath();
            }
        }

        Cluster[] clusters = Cluster.loadClusterInfo(fs, inputPath, K);

        //  /cluster 전부 삭제
        if (fs.exists(clusterPath)) {
            fs.delete(clusterPath, true);
        }

        // iteraion만듦
        FSDataOutputStream fos = fs.create(iterationPath);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));

        Vector<Double> vector;
        for (int index = 0; index < clusters.length; index++) {
            vector = clusters[index].getCentroid();
            for (int i = 0; i < vector.size() - 1; i++) {
                bw.write(vector.get(i) + ",");
            }
            bw.write(vector.get(vector.size() - 1) + "\n");
        }

        bw.close();
        fos.close();
        fs.close();

        return true;
    }

    //클러스터링하고 리셋을 해봅시당
    public boolean runIteration(Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = new Job(conf);

        job.setJarByClass(KMeansDriver.class);
        job.setMapperClass(KMeansMapper.class);
        job.setReducerClass(KMeansReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(getIterationPath(iteration)));

        return job.waitForCompletion(true);
    }

    public boolean runJob(Configuration conf) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = new Job(conf);

        job.setJarByClass(KMeansDriver.class);
        job.setMapperClass(KMeansClusteringMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setNumReduceTasks(0);
        return job.waitForCompletion(true);
    }

    private void parseArguments(String[] args, Configuration conf) throws IOException {

        logger.info("parseArguments");
        for (int i = 0; i < args.length; ++i) {
            if ("-i".equals(args[i]) || Constants.INPUT_PATH.equals(args[i])) {
                conf.set(Constants.INPUT_PATH, args[++i]);
                input = args[i];
            } else if ("-c".equals(args[i]) || Constants.CLUSTER_PATH.equals(args[i])) {
                conf.set(Constants.CLUSTER_PATH, args[++i]);
                conf.set(Constants.CLUSTER_ITERATOR, "0");
                cluster = args[i];
                iteration = 0;
            } else if ("-o".equals(args[i]) || Constants.OUTPUT_PATH.equals(args[i])) {
                conf.set(Constants.OUTPUT_PATH, args[++i]);
                output = args[i];
            } else if ("-k".equals(args[i]) || Constants.CLUSTER_COUNT.equals(args[i])) {
                conf.set(Constants.CLUSTER_COUNT, args[++i]);
                K = Integer.parseInt(args[i]);
            } else if ("-dm".equals(args[i]) || Constants.DISTANCE_MEASURE_KEY.equals(args[i])) {
                conf.set(Constants.DISTANCE_MEASURE_KEY, args[++i]);
            }

        }
    }
}