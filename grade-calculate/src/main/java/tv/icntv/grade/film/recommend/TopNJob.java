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

package tv.icntv.grade.film.recommend;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.sun.istack.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;
import tv.icntv.grade.film.core.AbstractJob;
import tv.icntv.grade.film.grade.num.*;
import tv.icntv.grade.film.utils.HadoopUtils;
import tv.icntv.grade.film.utils.MapReduceUtils;

import java.util.Date;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: xiaowu lei
 * Date: 13-12-11
 * Time: 上午9:38
 */
public class TopNJob extends AbstractJob {

    @Override
    public int run(String[] strings) throws Exception {
        Configuration configuration = getConf();
        configuration.setLong("mapred.min.split.size",512*1024*1024L);
        Job numJob = new Job(configuration, "calculate film program seed num job ");
        Path[] paths = getPaths(strings[0].split(","));
        HadoopUtils.deleteIfExist(strings[1]);
        MapReduceUtils.initMapperJob(NumCountMapper.class, Text.class, Text.class, this.getClass(), numJob, paths);
        //TableMapReduceUtil.initTableReducerJob(strings[1], NumCountReducer.class, numJob);
        MapReduceUtils.initReducerJob(new Path(strings[1]),NumCountReducer.class,numJob);
        numJob.waitForCompletion(true);
        Job programeSets = new Job(configuration,"calculate program set num job");
        HadoopUtils.deleteIfExist(strings[2]);
        MapReduceUtils.initMapperJob(NumProgramSetsMapper.class, Text.class, Text.class, this.getClass(), programeSets, new Path(strings[1]));
        programeSets.setCombinerClass(NumProgramSetCombiner.class);
        MapReduceUtils.initReducerJob(new Path(strings[2]), NumProgramSetsReducer.class,programeSets);
        return programeSets.waitForCompletion(true)?0:1;
//        return 0;
    }

    public static void main(String[] args) throws Exception {
        final Configuration configuration = HBaseConfiguration.create();
        configuration.addResource("grade.xml");
        String tables = configuration.get("hbase.cdn.tables");
        if (Strings.isNullOrEmpty(tables)) {
            return;
        }
        List<String> list = Lists.newArrayList(Splitter.on(",").split(tables));
        List<String> results = Lists.transform(list, new Function<String, String>() {
            @Override
            public String apply(@Nullable java.lang.String input) {
                return String.format(configuration.get("hdfs.directory.base.db"), new Date(), input);
            }
        });

        String[] arrays = new String[]{
                Joiner.on(",").join(results),
                String.format(configuration.get("hdfs.directory.num.middle"),new Date()),
                String.format(configuration.get("hdfs.directory.num.result"),new Date())
        };
        AbstractJob job = new TopNJob();
//        job.setStart(true);
        int i = ToolRunner.run(configuration, job, arrays);
        System.exit(i);
    }
}
