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

package tv.icntv.grade.film.correlate;

import com.alibaba.fastjson.JSON;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: xiaowu lei
 * Date: 13-12-2
 * Time: 下午1:42
 * * 010133501439929/media/new/2012/10/12/sd_zy_lmsezj068_20121012.ts{0} \t 观看时常{1} \t 流量{2} \t id`url`time(s)`name
 * `tag`category`year`directory`actors`zone`pptvCode`writer`programId \t ...
 * id`url`time(s)`name`tag`category`year`directory`actors`zone`pptvCode`writer`programId
 * 0 `1  ` 2     `3    ` 4  ` 5    `6   `  7      `   8  ` 9  `10     `  11`    12
 */
public class UserHistoryMapper extends Mapper<LongWritable,Text,Text,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if(Strings.isNullOrEmpty(value.toString())){
            return;
        }
        List<String> contents= Lists.newArrayList(Splitter.on("\t").omitEmptyStrings().split(value.toString()));
        Set<String> sets= Sets.newHashSet();
        for(int i=3;i<contents.size();i++){
              List<String> cs=Lists.newArrayList(Splitter.on("`").split(contents.get(i)));
            if(null == cs || cs.isEmpty() || cs.size()!=13){
                continue;
            }
            sets.add(cs.get(12));
        }
        context.write(new Text(contents.get(0).substring(0,15)),new Text(JSON.toJSONString(sets)));
    }
}
