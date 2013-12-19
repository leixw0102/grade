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
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.sun.xml.bind.api.TypeReference;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: xiaowu lei
 * Date: 13-12-2
 * Time: 下午1:43
 */
public class UserHistoryReducer extends Reducer<Text,Text,Text,NullWritable> {
    private final String TAB="\t";
    private final String groupName="icntv_correlate_group";
    private final String countName="icntv_count_name";
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<Text> vs= Lists.newArrayList(values.iterator());
        Set<String> ids= Sets.newHashSet();
        for(Text t : vs){
            Set<String> temp=JSON.parseObject(t.toString(), new com.alibaba.fastjson.TypeReference<Set<String>>(){});
            ids.addAll(temp);
        }
        org.apache.hadoop.mapreduce.Counter counter= context.getCounter(groupName,countName);
        counter.increment(1);
        context.write(new Text(Joiner.on(TAB).join(ids.iterator())), NullWritable.get());
    }
}
