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

import com.google.common.primitives.Floats;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import tv.icntv.grade.film.core.AbstractJob;
import tv.icntv.grade.film.utils.HadoopUtils;
import tv.icntv.grade.film.utils.MapReduceUtils;

import java.util.Date;

/**
 * Created with IntelliJ IDEA.
 * User: xiaowu lei
 * Date: 13-12-6
 * Time: 上午9:21
 */
public class CorrelateResultJob extends AbstractJob{

    @Override
    public int run(String[] strings) throws Exception {
        Configuration configuration = super.getConf();
        String output= strings[0]+"/frequentpatterns";
        long count= HadoopUtils.count(new Path(output), new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return path.getName().matches("part-r-\\d*");  //To change body of implemented methods use File | Settings | File Templates.
            }
        });
        System.out.println("count ="+count);
        if(count==0){
            return 1;
        }

        configuration.setLong("icntv.correlate.total.size",count);
        Job result = new Job(configuration,"correlate result calculate");
        MapReduceUtils.initMapperJob(CorrelateInputMapper.class, Text.class, Text.class, this.getClass(), result, new Path(output));
        result.setInputFormatClass(SequenceFileInputFormat.class);
//        TableMapReduceUtil.initTableReducerJob("");
        MapReduceUtils.initReducerJob(new Path(strings[1]),CorrelateOutPutReducer.class,result);
        result.waitForCompletion(true);
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public static void main(String[]args) throws Exception {

        Configuration configuration = HBaseConfiguration.create();
        configuration.addResource("grade.xml");

        if(null == args || args.length==0 ||args.length!=2){
            System.out.println("parameter <minSupport> <minConfidence> \r\n default minSupport=0.0002 \t minConfidence = 0.3");

        } else {
            configuration.set("correlate.result.minSup",args[0]);
            configuration.set("correlate.result.conf",args[1]);
        }
        ToolRunner.run(configuration,new CorrelateResultJob(),new String[]{
                String.format(configuration.get("icntv.correlate.fp.growth.output"),new Date()),
//                "patterns",
//                "output"});
        String.format(configuration.get("icntv.correlate.output"),new Date())});
    }
}
