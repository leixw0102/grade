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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import tv.icntv.grade.film.core.AbstractJob;
import tv.icntv.grade.film.utils.HadoopUtils;
import tv.icntv.grade.film.utils.MapReduceUtils;

import java.util.Date;

/**
 * Created with IntelliJ IDEA.
 * User: xiaowu lei
 * Date: 13-12-12
 * Time: 下午3:49
 */
public class DBCollectJob extends AbstractJob {
    @Override
    public int run(String[] strings) throws Exception {
        Configuration configuration = getConf();
        HadoopUtils.deleteIfExist(strings[1]);
//        System.out.print("delete "+strings[1]);
        Job db = new Job(configuration, "icntv db collect");
        MapReduceUtils.initMapperJob(DefaultHbaseMapper.class, Text.class, Text.class, this.getClass(), db, new Path(strings[0]));
        FileOutputFormat.setOutputPath(db, new Path(strings[1]));
        db.setNumReduceTasks(0);
        db.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) {
        Configuration configuration = HBaseConfiguration.create();
        configuration.addResource("grade.xml");
        String[] tables = new String[]{"chinacache_table_1"};
//        if (null != args && args.length != 0) {
//            tables = args[0].split(",");
//        } else {
//            tables = configuration.get("hbase.cdn.tables").split(",");
//        }
        System.out.print(configuration.get("film.base.msg"));
        for (String table : tables) {
            String db = String.format(configuration.get("hdfs.directory.base.db"), new Date(), table);
            String[] arrays = new String[]{
                    String.format(configuration.get("hdfs.directory.from.hbase"), new Date(), table),
                    db
            };
            try {
//                System.out.print(String.format(configuration.get("hdfs.directory.from.hbase"), new Date(), table)+"\t"+db);
                ToolRunner.run(configuration, new DBCollectJob(), arrays);
            } catch (Exception e) {
                continue;
            }
        }
    }
}
