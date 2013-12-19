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

package tv.icntv.grade.film;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tv.icntv.grade.film.core.AbstractJob;
import tv.icntv.grade.film.recommend.TopNJob;
import tv.icntv.grade.film.utils.ReflectionUtils;

import java.text.MessageFormat;
import java.util.Date;


/**
 * Created with IntelliJ IDEA.
 * User: xiaowu lei
 * Date: 13-11-5
 * Time: 下午1:29
 */
public class Grade extends AbstractJob {
    private static Logger logger = LoggerFactory.getLogger("grade-main");
    private static final String cdn_tables = "hbase.cdn.tables";

    private static final String split = ",";


    public Grade(Configuration conf) {
        this.configuration = conf;
    }

    protected String[] getTables() {
        String tables = configuration.get(cdn_tables);
        Preconditions.checkNotNull(null != tables && !tables.equals(""), "property=" + cdn_tables + " value null");
        return tables.split(split);
    }


    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        configuration.addResource("grade.xml");
        int i = ToolRunner.run(configuration, new Grade(configuration), new String[]{});
        System.exit(i);
    }

    @Override
    public int run(String[] strings) throws Exception {
        String[] tables = getTables();
        String source = "";
        // init data
        for (String table : tables) {
            String db = String.format(configuration.get("hdfs.directory.base.db"), new Date(), table);
            source += db + ",";
            String[] arrays = new String[]{table,//input table
                    String.format(configuration.get("hdfs.directory.from.hbase"), new Date(), table),
                    db
            };
            try {
               ToolRunner.run(configuration, (Tool) ReflectionUtils.newInstance(configuration.get("unit.table.job.className")), arrays);
            } catch (Exception e) {
                continue;
            }

        }
        //topN
        try{
            ToolRunner.run(configuration,new TopNJob(),new String[]{
                    source,
                    String.format(configuration.get("hdfs.directory.num.middle"),new Date()),
                    String.format(configuration.get("hdfs.directory.num.result"),new Date())
            });
        }catch (Exception e){
            return 1;
        }
        //recommend data generate data
        String baseCfData=String.format(configuration.get("hdfs.directory.base.score"),new Date());
        String output=String.format(configuration.get("icntv.cf.recommend.directory.target"), new Date());
        String temp=String.format(configuration.get("icntv.cf.recommend.directory.temp"), new Date());
        String optional = getParameterForCf(configuration, baseCfData,output,temp);
        try {
            ToolRunner.run(configuration, (Tool) ReflectionUtils.newInstance(configuration.get("recommend.job.className")),
                    new String[]{
                            source,
                            baseCfData,
                            optional ,
                            output,
                            temp
                    });
        } catch (Exception e) {
            return 1;
        }
        //关联
        String middleDirectory=String.format(configuration.get("icntv.correlate.input"), new Date());
        try {
            ToolRunner.run(configuration, (Tool) ReflectionUtils.newInstance(configuration.get("correlate.job.className")), new String[]
                    {
                            source,
                            middleDirectory,
                            getParameterForCorrelate(configuration,middleDirectory,String.format(configuration.get("icntv.correlate.fp.growth.output"),new Date())),
                            String.format(configuration.get("icntv.correlate.output"),new Date())
                    });
        } catch (Exception e) {
            return 1;
        }
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    private String getParameterForCf(Configuration configuration, String baseCfData,String output,String temp) {
        StringBuilder sb = new StringBuilder();
        sb.append("--input ").append(baseCfData);
        sb.append(" --output ").append(output);
        sb.append(" --numRecommendations ").append(configuration.get("icntv.cf.recommend.num"));
        sb.append(" --similarityClassname ").append(configuration.get("icntv.cf.recommend.similarityClassname"));
        sb.append(" --tempDir ").append(temp);
        return sb.toString();
    }

    private String getParameterForCorrelate(Configuration configuration,String correlateData,String fpGrowth){
        StringBuilder sb = new StringBuilder();
        sb.append("minSupport=").append(configuration.get("correlate.minSupport","3")).append("--")
                .append("maxHeapSize=100").append("--")
                .append("splitterPattern='[\t ]'").append("--")
                .append("input=").append(correlateData).append("--")
                .append("output=").append(fpGrowth);
        return sb.toString();
    }


}
