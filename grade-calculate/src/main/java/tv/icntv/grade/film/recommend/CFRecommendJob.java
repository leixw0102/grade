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
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.cf.taste.hadoop.item.RecommenderJob;
import tv.icntv.grade.film.core.AbstractJob;
import tv.icntv.grade.film.grade.time.TimeCombiner;
import tv.icntv.grade.film.grade.time.TimeMaper;
import tv.icntv.grade.film.grade.time.TimeReducer;
import tv.icntv.grade.film.utils.HadoopUtils;
import tv.icntv.grade.film.utils.MapReduceUtils;

import java.text.MessageFormat;
import java.util.Date;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: xiaowu lei
 * Date: 13-11-26
 * Time: 下午4:23
 */
public class CFRecommendJob extends AbstractJob {

    @Override
    public int run(String[] strings) throws Exception {
        Configuration configuration = getConf();
        configuration.setLong("mapred.min.split.size",512*1024*1024L);
        HadoopUtils.deleteIfExist(strings[1]);
        Job timeJob = new Job(configuration, "calculate film time middle job");
        MapReduceUtils.initMapperJob(TimeMaper.class, Text.class, Text.class, this.getClass(), timeJob, getPaths(strings[0].split(",")));
        timeJob.setCombinerClass(TimeCombiner.class);
        MapReduceUtils.initReducerJob(new Path(strings[1]),TimeReducer.class,timeJob);
        timeJob.waitForCompletion(true);

        HadoopUtils.deleteIfExist(strings[3]);
        HadoopUtils.deleteIfExist(strings[4]);
        return ToolRunner.run(configuration,new RecommenderJob(),strings[2].split(" "));
    }

    public static void main(String[]args) throws Exception {
        final Configuration configuration=HBaseConfiguration.create();
        configuration.addResource("grade.xml");
        String baseCfData=String.format(configuration.get("hdfs.directory.base.score"),new Date());
        String output=String.format(configuration.get("icntv.cf.recommend.directory.target"),new Date());
        String temp=String.format(configuration.get("icntv.cf.recommend.directory.temp"), new Date());
        StringBuilder sb = new StringBuilder();
        sb.append("--input ").append(baseCfData);
        sb.append(" --output ").append(output);
        sb.append(" --numRecommendations ").append(configuration.get("icntv.cf.recommend.num"));
        sb.append(" --similarityClassname ").append(configuration.get("icntv.cf.recommend.similarityClassname"));
        sb.append(" --tempDir ").append(temp);

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

        int i=ToolRunner.run(configuration,new CFRecommendJob(),new String[]{
                Joiner.on(",").join(results),
                baseCfData,
                sb.toString(),
                output ,
                temp
        });
        System.exit(i);
    }
}
