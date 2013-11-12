package org.apache.hadoop.KMeans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Vector;

/**
 * Created with IntelliJ IDEA.
 * User: YeonjuMac
 * Date: 13. 9. 5.
 * Time: 오전 9:40
 * To change this template use File | Settings | File Templates.
 */
public class Cluster {

    private Vector<Double> centroid = null;
    private Vector<Double> sum = null;
    private int count = 0;

    public static Cluster[] initializeClusters(int K) {
        Cluster[] clusters = new Cluster[K];
        for (int index = 0; index < K; ++index) {
            clusters[index] = new Cluster();
        }
        return clusters;
    }

    public static Cluster[] loadClusterInfo(Configuration conf, Path clusterPath, int K) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        return loadClusterInfo(fs, clusterPath, K);
    }

    public static Cluster[] loadClusterInfo(FileSystem fs, Path clusterPath, int K) throws IOException {

        Cluster[] clusters = Cluster.initializeClusters(K);

        FSDataInputStream fis = fs.open(clusterPath);
        BufferedReader br = new BufferedReader(new InputStreamReader(fis));

        // a,b,c,d,e,d,f...
        String line;

        int index = 0;
        while ((line = br.readLine()) != null && index < clusters.length) {
            Vector vector = KMeansUtil.decodeVector(line.split(Constants.DELIMITER));
            Cluster cluster = clusters[index];
            cluster.setCentroid(vector);
            ++index;
        }

        br.close();
        fis.close();
        return clusters;
    }

    public static int findNearestCentroidIndex(Vector<Double> point, Cluster[] clusters) {
        double min = Double.MAX_VALUE;
        int store_number = 0;
        for (int i = 0; i < clusters.length; i++) {
            double distance = getDistance(point, clusters[i].getCentroid());
            if (min > distance) {
                min = distance;
                store_number = i;
            }
        }
        return store_number;
    }

    public static double getDistance(Vector<Double> p1, Vector<Double> p2) {
        double result = 0.0;
        for (int i = 0; i < p1.size(); i++) {
            result += Math.pow((p1.get(i) - p2.get(i)), 2);
        }
        return Math.sqrt(result);
    }

    void setCentroid(Vector<Double> centroid) {
        this.centroid = centroid;
    }

    Vector<Double> getCentroid() {
        return this.centroid;
    }

    Vector<Double> getCentroidWithCompute() {
        if (sum != null) {
            for (int i = 0; i < sum.size(); i++) {
                centroid.set(i, sum.get(i) / count);
            }
        }
        return centroid;
    }
    public static String getOutputString(Cluster[] clusters) {
        String output = "";
        Vector<Double> vector;
        for(int index = 0 ;index < clusters.length ; index++) {
            vector = clusters[index].getCentroid();
            for(int i = 0 ;i < vector.size() - 1; i++) {
                output += vector.get(i) + Constants.DELIMITER;
            }
            output += vector.get(vector.size()-1) + "\n";
        }
        return output;
    }

    public void addPoint(Vector<Double> point) {
        String temp;
        count++;
        if (sum != null) {
            for (int i = 0; i < point.size(); i++) {
                temp = point.get(i).toString();
                if (KMeansUtil.isNumeric(temp)) {
                    sum.set(i, Double.parseDouble(sum.get(i).toString()) + Double.parseDouble(temp));
                }
            }
        } else {
            sum = point;
        }
    }

}
