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

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: xiaowu lei
 * Date: 13-11-28
 * Time: 上午11:30
 */
public class CFRecommendInitMapper extends TableMapper<Text,NullWritable> {
    private String FAMILY="base";
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        String userId=Bytes.toString(value.getValue(Bytes.toBytes(FAMILY), Bytes.toBytes("userId")));
        String itemId= Bytes.toString(value.getValue(Bytes.toBytes(FAMILY),Bytes.toBytes("itemId")));
        String score=Bytes.toString(value.getValue(Bytes.toBytes(FAMILY),Bytes.toBytes("score")));
        context.write(new Text(userId+","+itemId+","+score),NullWritable.get());
    }
}
