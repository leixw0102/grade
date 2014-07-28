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

package tv.icntv.grade.film.grade.time;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Longs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import tv.icntv.grade.film.utils.MathExtend;

import java.io.IOException;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: xiaowu lei
 * Date: 13-11-6
 * Time: 下午4:21
 */
public class TimeReducer extends Reducer<Text, Text, Text,Text> {
//public class TimeReducer extends Reducer<Text,Text,Text,Text> {
    private static final String scoreKey = "grade.total.score";
    private double scoreValue=10.0;
    String pattern="\\d*";
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
//        System.out.println(configuration.get("mapred.task.timeout"));
        configuration.setLong("mapred.task.timeout",6000000L);
//        System.out.println("setup configuration"+configuration.get("mapred.task.timeout"));
        scoreValue = Double.parseDouble(configuration.get(scoreKey, "10.0"));
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String k = key.toString();
        if (Strings.isNullOrEmpty(k) || k.length()<=15) {
            return;
        }
        String deviceId=k.substring(0,15);
        if(!deviceId.matches(pattern)){
            return;
        }
        String itemId=k.substring(15, k.length());
        Long temp  = Long.parseLong(itemId);
        if(null ==temp ){
            return;
        }
        double timeScore = 0.0;
        List<Text> list = Lists.newArrayList(values);
        for (Text it : list) {
            timeScore += Double.parseDouble(it.toString());

        }
        double score = MathExtend.divide(timeScore, list.size(), 3) * scoreValue;
//        Put put = new Put(Bytes.toBytes(k));
//        put.add(Bytes.toBytes("base"), Bytes.toBytes("userId"), Bytes.toBytes(k.substring(0, 15)));
//        put.add(Bytes.toBytes("base"), Bytes.toBytes("itemId"), Bytes.toBytes(k.substring(15, k.length())));
//        put.add(Bytes.toBytes("base"), Bytes.toBytes("timeScore"), Bytes.toBytes(score + ""));
//        context.progress();
//        context.write(new ImmutableBytesWritable(put.getRow()), put);

        context.write(new Text(k.substring(0, 15)),new Text(itemId+"\t"+score));
    }

//    private void process(long start,Context context){
//        long end = System.nanoTime();
//        long middle=(end-start)/1000;
//        if(middle>20*60){
//            context.getProgress();
//        }
//    }
}
