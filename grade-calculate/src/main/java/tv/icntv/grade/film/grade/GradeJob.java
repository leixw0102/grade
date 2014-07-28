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

package tv.icntv.grade.film.grade;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.sun.istack.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.util.ToolRunner;
import tv.icntv.grade.film.correlate.UserHistoryMapper;
import tv.icntv.grade.film.correlate.UserHistoryReducer;
import tv.icntv.grade.film.core.AbstractJob;
import tv.icntv.grade.film.grade.num.NumCountMapper;
import tv.icntv.grade.film.grade.num.NumCountReducer;
import tv.icntv.grade.film.grade.time.TimeCombiner;
import tv.icntv.grade.film.grade.time.TimeMaper;
import tv.icntv.grade.film.grade.time.TimeReducer;
import tv.icntv.grade.film.utils.HadoopUtils;
import tv.icntv.grade.film.utils.MapReduceUtils;

import java.io.IOException;
import java.util.Date;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: xiaowu lei
 * Date: 13-11-25
 * Time: 下午1:52
 */
@Deprecated
public class GradeJob extends AbstractJob {
    private final String groupName="icntv_correlate_group";
    private final String countName="icntv_count_name";


    @Override
    public int run(String[] strings) throws Exception {
        Configuration configuration = this.getConf();
        //num  job
        Job numJob = new Job(configuration, "calculate film seed num job ");
        Path[] paths = getPaths(strings[0].split(","));
        MapReduceUtils.initMapperJob(NumCountMapper.class, Text.class, LongWritable.class, this.getClass(), numJob, paths);
//        TableMapReduceUtil.initTableReducerJob(strings[1], NumCountReducer.class, numJob);
        ControlledJob controlledJob3 = new ControlledJob(configuration);
        controlledJob3.setJob(numJob);
        //time  job
        Job timeJob = new Job(configuration, "calculate film time middle job");
        MapReduceUtils.initMapperJob(TimeMaper.class, Text.class, Text.class, this.getClass(), timeJob, paths);
        timeJob.setCombinerClass(TimeCombiner.class);
        MapReduceUtils.initReducerJob(new Path(strings[2]),TimeReducer.class,timeJob);

        ControlledJob controlledJob4 = new ControlledJob(configuration);
        controlledJob4.setJob(timeJob);

//        //关联
        Job correlate = new Job(configuration, "icntv correlate job");
        MapReduceUtils.initMapperJob(UserHistoryMapper.class, Text.class, Text.class, this.getClass(), correlate, paths);
        MapReduceUtils.initReducerJob(new Path(strings[3]), UserHistoryReducer.class, correlate);
        ControlledJob correlateController = new ControlledJob(configuration);
        correlateController.setJob(correlate);
//        controlledJob3.getDependentJobs().add()

        JobControl jobControl = new JobControl("unit grade");

        jobControl.addJob(controlledJob3);
        jobControl.addJob(controlledJob4);
//        jobControl.addJob(controlledJob5);
        jobControl.addJob(correlateController);
        new Thread(jobControl).start();
        while (!jobControl.allFinished()) {
            Thread.sleep(5000);
        }
//        Counter counter =correlate.getCounters().findCounter(groupName,countName);
//        HadoopUtils.save(new Path(configuration.get("icntv.temp.file")),counter.getValue());
        return 0;
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
                configuration.get("film.see.num.table"),
                String.format(configuration.get("hdfs.directory.base.score"),new Date()),
                String.format(configuration.get("icntv.correlate.input"), new Date())
        };
        int i = ToolRunner.run(configuration, new GradeJob(), arrays);
        System.exit(i);
    }
}
