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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Vector;


public class KMeansReducer extends Reducer<Text, Text, Text, Text> {

    private int K;
    private Text result = new Text();
    private Cluster[] clusters;
    private int iterator;
    private Path clusterPath;

    private final static String DELIMITER = ",";
    int clusterID;
    String column;
    String[] columns;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();

        K = Integer.parseInt(conf.get(Constants.CLUSTER_COUNT, "1"));
        iterator = Integer.parseInt(conf.get(Constants.CLUSTER_ITERATOR)) - 1;
        clusterPath = new Path(conf.get(Constants.CLUSTER_PATH) + "/centroid_" + iterator + "/part-r-00000");

        FileSystem fs = FileSystem.get(conf);
        clusters = Cluster.loadClusterInfo(fs, clusterPath, K);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //여기서 centroid계산함
        clusterID = Integer.parseInt(key.toString());

        for(Text value : values) {
            column = value.toString();
            columns = column.split(DELIMITER);
            clusters[clusterID].addPoint(KMeansUtil.decodeVector(columns));
        }

        Vector<Double> vector = clusters[clusterID].getCentroidWithCompute();
        String output = "";
        for(int i = 0 ;i < vector.size()-1 ; i++){
            output += vector.get(i).intValue() + ",";
        }
        output += (vector.get(vector.size()-1)).intValue() ;

        result.set(output);

        context.write(result, null);
    }
}


