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

package tv.icntv.grade.film.dbcollect.db;

import com.alibaba.fastjson.JSON;
import icntv.cache.IRedisCache;
import icntv.cache.Redis;
import icntv.exception.CacheExecption;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import redis.clients.jedis.Jedis;
import tv.icntv.grade.film.dbcollect.DBLoadThread;
import tv.icntv.grade.film.dbcollect.bean.FilmMsg;
import tv.icntv.grade.film.dbcollect.bean.Films;

import java.io.IOException;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: xiaowu lei
 * Date: 13-12-3
 * Time: 下午2:52
 */
@Deprecated
public class TableDBReducer extends TableReducer<Text, Text,ImmutableBytesWritable> {
    private DBLoadThread loadThread=null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        loadThread=DBLoadThread.getInstance();
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
          if(null == key){
              return;
          }
        List<FilmMsg> list=loadThread.execute(new DBLoad(new String[]{key.toString()},key.toString()));
        if(null == list || list.isEmpty()){
            return;
        }

        Put put = new Put(key.getBytes());
        put.add(Bytes.toBytes("base"),Bytes.toBytes("msg"), Bytes.toBytes(JSON.toJSONString(list)));
        context.write(new ImmutableBytesWritable(key.getBytes()),put);
    }
}
