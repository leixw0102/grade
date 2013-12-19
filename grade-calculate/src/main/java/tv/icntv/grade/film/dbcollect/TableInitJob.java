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

package tv.icntv.grade.film.dbcollect;

import com.google.common.base.Preconditions;
import com.google.common.io.Closeables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PageFilter;

import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.lucene.analysis.util.FilesystemResourceLoader;
import tv.icntv.grade.film.core.AbstractJob;
import tv.icntv.grade.film.dbcollect.db.TableDBMapper;
import tv.icntv.grade.film.dbcollect.db.TableDBReducer;
import tv.icntv.grade.film.utils.HadoopUtils;
import tv.icntv.grade.film.utils.MapReduceUtils;
import tv.icntv.grade.film.utils.ReflectionUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Date;

/**
 * Created with IntelliJ IDEA.
 * User: xiaowu lei
 * Date: 13-11-18
 * Time: 下午2:25

 */
@Deprecated
public class TableInitJob extends AbstractJob {

    @Override
    public int run(String[] strings) throws Exception {

        Configuration configuration = getConf();
        //table job
        Job tableJob = new Job(configuration, "icntv grade init");
        Scan scan = new Scan();

        HadoopUtils.deleteIfExist(strings[1]);
        HadoopUtils.deleteIfExist(strings[2]);
        TableMapReduceUtil.initTableMapperJob(strings[0], scan, TableInitMapper.class, Text.class, Text.class, tableJob);
        MapReduceUtils.initReducerJob(new Path(strings[1]), TableInitReducer.class, tableJob);
        tableJob.waitForCompletion(true);
        Job db = new Job(configuration,"icntv db collect");
        configuration.setLong("mapred.min.split.size",512*2014*1024L);
        MapReduceUtils.initMapperJob(DefaultHbaseMapper.class,Text.class,Text.class,this.getClass(),db,new Path(strings[1]));
        FileOutputFormat.setOutputPath(db,new Path(strings[2]));
        db.setNumReduceTasks(0);
        db.waitForCompletion(true);
        return 0;
    }
    public static void main(String[] args) {
       Configuration configuration=HBaseConfiguration.create();
        configuration.addResource("grade.xml");
        String[] tables=configuration.get("hbase.cdn.tables").split(",");
        for(String table : tables){
            String db = String.format(configuration.get("hdfs.directory.base.db"), new Date(), table);
            String[] arrays = new String[]{table,//input table
                    String.format(configuration.get("hdfs.directory.from.hbase"), new Date(), table),
                    db,
                    configuration.get("film.base.msg")
            };
            try {
                ToolRunner.run(configuration, new TableInitJob(), arrays);
            } catch (Exception e) {
                continue;
            }
        }
    }
//    protected String[] getTables() {
//        String tables = configuration.get(cdn_tables);
//        Preconditions.checkNotNull(null != tables && !tables.equals(""), "property=" + cdn_tables + " value null");
//        return tables.split(split);
//    }
}
