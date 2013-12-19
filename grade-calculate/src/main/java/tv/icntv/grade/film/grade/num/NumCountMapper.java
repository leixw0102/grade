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

package tv.icntv.grade.film.grade.num;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: xiaowu lei
 * Date: 13-11-22
 * Time: 上午11:35
 * 影片观看次数
 * file
 * 010133501439929/media/new/2012/10/12/sd_zy_lmsezj068_20121012.ts{0} \t 观看时常{1} \t 流量{2} \t id`url`time(s)`name
 * `tag`category`year`directory`actors`zone`pptvCode`writer`programId \t ...
 * id`url`time(s)`name`tag`category`year`directory`actors`zone`pptvCode`writer`programId
 * 0 `1  ` 2     `3    ` 4  ` 5    `6   `  7      `   8  ` 9  `10     `  11`    12
 * <p/>
 * output key ==url.
 * output value -->Map<节目id,List<节目集id>>
 */
public class NumCountMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (null == value) {
            return;
        }
        String[] values = value.toString().split("\t");
        if (null == values || values.length < 4) {
            return;
        }

        Map<Long, List<Long>> maps = Maps.newHashMap();
        String outputkey=values[3].split("`")[1];

        for (int i = 3; i < values.length; i++) {
            String[] dbValue = values[i].split("`");
            if (null == dbValue || dbValue.length != 13) {
                continue;
            }
            long id = Long.parseLong(dbValue[0]);
            long programId = Long.parseLong(dbValue[12]);
            List<Long> vs=null;
            if (maps.containsKey(id)) {
                vs = maps.get(id);
                vs.add(programId);
                maps.put(id, vs);
            } else {
                vs= Lists.newArrayList();
                vs.add(programId);
                maps.put(id,vs);
            }
        }
        context.progress();
        context.write(new Text(outputkey), new Text(JSON.toJSONString(maps)));
    }
}
