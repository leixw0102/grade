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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: xiaowu lei
 * Date: 13-12-12
 * Time: 上午11:27
 */
public class HadoopUtilsTest {
    public static void main(String[]args) throws IOException {
        Configuration confgiruration=HBaseConfiguration.create();
        FileSystem fileSystem=null;
        try {
            fileSystem=FileSystem.get(confgiruration);
            FileStatus[] fileStatuses=fileSystem.listStatus(new Path("/icntv/grade/correlate-result/2013-12-12"),new PathFilter() {
                @Override
                public boolean accept(Path path) {

                    return path.getName().matches("part-r-\\d*");
                }
            });
            for(FileStatus f : fileStatuses){
                IOUtils.copyBytes(fileSystem.open(f.getPath()),System.out,4096,false);
            }
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            if(null != fileSystem){
                fileSystem.close();
            }
        }
    }
}
