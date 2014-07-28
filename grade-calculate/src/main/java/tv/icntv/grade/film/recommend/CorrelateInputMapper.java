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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.common.Pair;
import org.apache.mahout.fpm.pfpgrowth.convertors.string.TopKStringPatterns;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: xiaowu lei
 * Date: 13-12-6
 * Time: 上午9:17
 */
public class CorrelateInputMapper extends Mapper<Text,TopKStringPatterns,Text,Text> {
    float minSup=0f;
    float minConf=0f;
    long totalSize=0L;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        minSup= Float.parseFloat(configuration.get("correlate.result.minSup","0.0002"));
        minConf=Float.parseFloat(configuration.get("correlate.result.conf", "0.03"));
        totalSize=configuration.getLong("icntv.correlate.total.size",0L);
        System.out.println(totalSize+"\t"+minSup+"\t"+minConf);
    }

    @Override
    protected void map(Text key, TopKStringPatterns value, Context context) throws IOException, InterruptedException {
        long firstFrequencyItem = -1;
        String firstItemId = null;
        List<Pair<List<String>, Long>> patterns = value.getPatterns();

        int i = 0;
        for (Pair<List<String>, Long> pair : patterns) {
            List<String> itemList = pair.getFirst();
            Long occurrence = pair.getSecond();
            if (i == 0) {
                firstFrequencyItem = occurrence;
                firstItemId = itemList.get(0);
                if(null == firstItemId){
                    continue;
                }
            } else {
                double support = (double) occurrence / totalSize;
                double confidence = (double) occurrence / firstFrequencyItem;
                if ((support > minSup
                        && confidence > minConf)) {
                    if(null == firstItemId){
                        continue;
                    }
                    for (String itemId : itemList) {
                        if (!itemId.equals(firstItemId)) {
                             context.write(new Text(itemId),new Text(firstItemId));
                        }
                    }

                }
            }
            i++;
        }
    }
}
