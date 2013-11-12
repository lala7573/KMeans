/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.KMeans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Vector;


public class KMeansClusteringMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text result = new Text();
    private int K;
    private Cluster[] clusters;
    private int iterator;
    private Path clusterPath;   //센트로이드 정보가 저장되어있는 파일 위치

    @Override
    protected void setup(Context context) {
        try {
            Configuration conf = context.getConfiguration();

            K = Integer.parseInt(conf.get(Constants.CLUSTER_COUNT, "1"));
            iterator = Integer.parseInt(conf.get(Constants.CLUSTER_ITERATOR)) -1;
            clusterPath = new Path(conf.get(Constants.CLUSTER_PATH) + "/centroid_" + iterator + "/part-r-00000");

            FileSystem fs = FileSystem.get(conf);
            clusters = Cluster.loadClusterInfo(fs, clusterPath, K);
        } catch (Exception e) {
            e.printStackTrace();
            //throw new IllegalArgumentException("Invalid Parameter", e);
        }
    }

    @Override
    protected void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
        String[] columns = values.toString().split(Constants.DELIMITER);
        Vector<Double> point = KMeansUtil.decodeVector(columns);
        int mValue = Cluster.findNearestCentroidIndex(point, clusters);

        result.set(String.valueOf(mValue));
        if (columns != null && columns.length > 0) {
            context.write(values, result);            // cluster index = result,
        }
    }
}









