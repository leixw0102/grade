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

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: xiaowu lei
 * Date: 13-12-18
 * Time: 下午4:42
 */
public class NumProgramSetsMapper extends Mapper<LongWritable,Text,Text,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        if(value==null || Strings.isNullOrEmpty(value.toString())){
            return;
        }
        List<String> vs = Lists.newArrayList(Splitter.on("\t").split(value.toString()));
        if(null == vs || vs.isEmpty() || vs.size()!=4){
            System.out.println("value format error ");
            return;
        }
        String programeSets= vs.get(2);
        String num = vs.get(3);
        if(Strings.isNullOrEmpty(programeSets)){
            return;
        }
        Iterable<String> iterable=Splitter.on(CharMatcher.is(',').or(CharMatcher.is('`'))).split(programeSets);
        for(Iterator<String> it = iterable.iterator();it.hasNext();){
            String k=it.next();
             context.write(new Text(k),new Text(num));
        }

    }
}
