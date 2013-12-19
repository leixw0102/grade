/* Copyright 2013 Future TV, Inc.
 *
 *      Licensed under the Apache License, Version 2.0 (the "License");
 *      you may not use this file except in compliance with the License.
 *      You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *      Unless required by applicable law or agreed to in writing, software
 *      distributed under the License is distributed on an "AS IS" BASIS,
 *      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *      See the License for the specific language governing permissions and
 *      limitations under the License.
 */

package tv.icntv.grade.film.utils;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: xiaowu lei
 * Date: 13-11-21
 * Time: 下午5:21
 */
public class MapReduceUtils {

    public static void initMapperJob(
                                     Class<? extends Mapper> mapper,
                                     Class<? extends Writable> outputKeyClass,
                                     Class<? extends Writable> outputValueClass,
                                     Class jar,

                                     Job job ,
                                     Path ...input) throws IOException {
        job.setJarByClass(jar);
        if (null != outputKeyClass) {
            job.setMapOutputKeyClass(outputKeyClass);
        }
        if (null != outputValueClass) {
            job.setMapOutputValueClass(outputKeyClass);
        }
        job.setMapperClass(mapper);
        FileInputFormat.setInputPaths(job,input);

    }

    public static void initReducerJob(Path output,
                                      Class<? extends Reducer> reducer,
                                      Class<? extends Writable> outputKeyClass,
                                      Class<? extends Writable> outputValueClass,
                                      int numReducerTask,
                                      Job job) {
        if (null != outputKeyClass) {
            job.setMapOutputKeyClass(outputKeyClass);
        }
        if (null != outputValueClass) {
            job.setMapOutputValueClass(outputKeyClass);
        }
        job.setReducerClass(reducer);
        if (numReducerTask > 0 && null != output) {
            job.setNumReduceTasks(numReducerTask);
            FileOutputFormat.setOutputPath(job, output);
        }
        if (numReducerTask == 0) {
            job.setNumReduceTasks(0);
        }
        if (numReducerTask < 0 && null != output) {
            FileOutputFormat.setOutputPath(job, output);
        }
    }

    public static void initReducerJob(Path output,
                                      Class<? extends Reducer> reducer,

                                      Job job) {
        job.setReducerClass(reducer);
        FileOutputFormat.setOutputPath(job, output);
    }
}
