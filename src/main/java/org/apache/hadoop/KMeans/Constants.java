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

/**
 * MapReduce Job 상수.
 *
 */
public class Constants {
    final static int JOB_SUCCESS= 1;
    final static int JOB_FAIL=0;

    final static String DELIMITER = ",";

    final static String INPUT_PATH = "-input";
    final static String OUTPUT_PATH = "-output";
    final static String CLUSTER_PATH = "-cluster";
    final static String CLUSTER_COUNT = "-k";
    final static String CLUSTER_ITERATOR = "-iter";
    final static String DISTANCE_MEASURE_KEY = "-dm";

}
