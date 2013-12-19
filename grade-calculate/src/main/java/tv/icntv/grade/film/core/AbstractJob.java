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

package tv.icntv.grade.film.core;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: xiaowu lei
 * Date: 13-11-5
 * Time: 下午1:41
 * init job ,grade job;recommend job
 */
public abstract  class AbstractJob extends Configured implements Tool {
    protected boolean start=false;
    protected Logger logger = LoggerFactory.getLogger(getClass());
    protected Configuration configuration = null;
    public boolean isStart() {
        return start;
    }

    public void setStart(boolean start) {
        this.start = start;
    }

    @Override
    public void setConf(Configuration conf) {
        super.setConf(conf);
    }
    protected Path[] getPaths(String[] files) throws IOException {
        List<Path> paths = Lists.newArrayList();
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(super.getConf());
            for (String file : files) {
                Path p = new Path(file);
                if (fileSystem.exists(p)) {
                    paths.add(p);
                }
            }
        } catch (Exception e) {

        } finally {
            if (null != fileSystem) {
                fileSystem.close();
            }
        }
        return paths.toArray(new Path[paths.size()]);
    }
    @Override
    public Configuration getConf() {
        Configuration configuration=super.getConf();
        if(null == configuration){
            configuration= HBaseConfiguration.create();
            configuration.addResource("grade.xml");
        }
        return configuration;    //To change body of overridden methods use File | Settings | File Templates.
    }


}
