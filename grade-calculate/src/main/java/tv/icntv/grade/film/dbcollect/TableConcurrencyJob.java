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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import tv.icntv.grade.film.core.AbstractJob;
import tv.icntv.grade.film.utils.HadoopUtils;
import tv.icntv.grade.film.utils.MapReduceUtils;

import java.util.Date;

/**
 * Created with IntelliJ IDEA.
 * User: xiaowu lei
 * Date: 13-12-19
 * Time: 上午10:36
 */
public class TableConcurrencyJob extends AbstractJob {

    @Override
    public int run(String[] strings) throws Exception {
        Configuration configuration = getConf();
        String[] tables = configuration.get("hbase.cdn.tables").split(",");
        JobControl jobControl = new JobControl("data init");
        for (String table : tables) {
            //
            String hbaseDbDirectory = String.format(configuration.get("hdfs.directory.from.hbase"), new Date(), table);
            HadoopUtils.deleteIfExist(hbaseDbDirectory);
            Job tableJob = new Job(configuration, "icntv grade init " + table);
            TableMapReduceUtil.initTableMapperJob(table, new Scan(), TableInitMapper.class, Text.class, Text.class, tableJob);
            MapReduceUtils.initReducerJob(new Path(hbaseDbDirectory), TableInitReducer.class, tableJob);
            // controlled job
            ControlledJob tableControlledJob = new ControlledJob(configuration);
            tableControlledJob.setJob(tableJob);

            String dbDirectory = String.format(configuration.get("hdfs.directory.base.db"), new Date(), table);
            HadoopUtils.deleteIfExist(dbDirectory);
            Configuration conf = getConf();
            Job db = new Job(conf, "icntv db collect " + table);
            conf.setLong("mapred.min.split.size", 512 * 2014 * 1024L);
            MapReduceUtils.initMapperJob(DefaultHbaseMapper.class, Text.class, Text.class, this.getClass(), db, new Path(strings[1]));
            FileOutputFormat.setOutputPath(db, new Path(dbDirectory));
            db.setNumReduceTasks(0);
            ControlledJob dbControlledJob = new ControlledJob(conf);
            dbControlledJob.setJob(db);
            dbControlledJob.addDependingJob(tableControlledJob);
            //controlledJob.
            jobControl.addJob(tableControlledJob);
            jobControl.addJob(dbControlledJob);
        }
        new Thread(jobControl).start();
        while (!jobControl.allFinished()) {
            Thread.sleep(5000);
        }
        return 0;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        configuration.addResource("grade.xml");
        ToolRunner.run(configuration, new TableInitJob(), args);
    }
}
