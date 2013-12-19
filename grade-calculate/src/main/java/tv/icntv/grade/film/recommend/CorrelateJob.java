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
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.Parameters;
import org.apache.mahout.fpm.pfpgrowth.FPGrowthDriver;
import org.apache.mahout.fpm.pfpgrowth.PFPGrowth;
import org.jruby.util.Join;
import tv.icntv.grade.film.core.AbstractJob;
import tv.icntv.grade.film.correlate.UserHistoryMapper;
import tv.icntv.grade.film.correlate.UserHistoryReducer;
import tv.icntv.grade.film.utils.HadoopUtils;
import tv.icntv.grade.film.utils.MapReduceUtils;

import java.util.Date;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: xiaowu lei
 * Date: 13-12-2
 * Time: 下午4:47
 */
public class CorrelateJob extends AbstractJob {

    @Override
    public int run(String[] strings) throws Exception {
        Configuration configuration=getConf();
        HadoopUtils.deleteIfExist(strings[1]);
        Job correlate = new Job(configuration, "icntv correlate job");
        MapReduceUtils.initMapperJob(UserHistoryMapper.class, Text.class, Text.class, this.getClass(), correlate, getPaths(strings[0].split(",")));
        MapReduceUtils.initReducerJob(new Path(strings[1]), UserHistoryReducer.class, correlate);
        if(!correlate.waitForCompletion(true)){
            return 1;
        };
        Parameters parameter =getParameter(strings[2]);
        HadoopUtils.deleteIfExist(parameter.get("output"));
        PFPGrowth.runPFPGrowth(parameter,configuration);
        String output= parameter.get("output")+"/frequentpatterns";
        long count= HadoopUtils.count(new Path(output), new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return path.getName().matches("part-r-\\d*");  //To change body of implemented methods use File | Settings | File Templates.
            }
        });
        if(count==0){
            return 1;
        }
        configuration.setLong("icntv.correlate.total.size",count);
        HadoopUtils.deleteIfExist(strings[3]);
        Job result = new Job(configuration,"correlate result calculate");
        MapReduceUtils.initMapperJob(CorrelateInputMapper.class, Text.class, Text.class, this.getClass(), result, new Path(output));
        result.setInputFormatClass(SequenceFileInputFormat.class);
//        TableMapReduceUtil.initTableReducerJob("");
        MapReduceUtils.initReducerJob(new Path(strings[3]),CorrelateOutPutReducer.class,result);
        result.waitForCompletion(true);
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    private Parameters getParameter(String strings) {
        Parameters parameters=new Parameters();
        String[]values=strings.split("--");
        for(String v:values){
            String[] kvs=v.split("=");
            if(null == kvs || kvs.length!=2){
                continue;
            }
            parameters.set(kvs[0],kvs[1]);
        }
        return parameters;  //To change body of created methods use File | Settings | File Templates.
    }
    public static void main(String[]args) throws Exception {
        final Configuration configuration= HBaseConfiguration.create();
        configuration.addResource("grade.xml");
        String tables = configuration.get("hbase.cdn.tables");
        if (Strings.isNullOrEmpty(tables)) {
            return;
        }
        List<String> list = Splitter.on(",").splitToList(tables);
        List<String> results = Lists.transform(list, new Function<String, String>() {
            @Override
            public String apply(@Nullable java.lang.String input) {
                return String.format(configuration.get("hdfs.directory.base.db"), new Date(), input);
            }
        });
        String middleDirectory=String.format(configuration.get("icntv.correlate.input"), new Date());
        StringBuilder sb = new StringBuilder();
        sb.append("minSupport=").append(configuration.get("correlate.minSupport","3")).append("--")
                .append("maxHeapSize=100").append("--")
                .append("splitterPattern='[\t ]'").append("--")
                .append("input=").append(middleDirectory).append("--")
                .append("output=").append(String.format(configuration.get("icntv.correlate.fp.growth.output"),new Date()));
        ToolRunner.run(configuration,new CorrelateJob(),new String[]{
                Joiner.on(",").join(results),
                middleDirectory,
                sb.toString(),
                String.format(configuration.get("icntv.correlate.output"),new Date())
        });
    }
}
