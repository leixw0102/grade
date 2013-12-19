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

package tv.icntv.grade.film.dbcollect.hbase;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.TypeReference;
import com.google.common.base.Strings;
import icntv.cache.IRedisCache;
import icntv.cache.Redis;
import icntv.exception.CacheExecption;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import redis.clients.jedis.Jedis;
import tv.icntv.grade.film.core.AbstractDB;
import tv.icntv.grade.film.core.AbstractJob;
import tv.icntv.grade.film.dbcollect.DBLoadThread;
import tv.icntv.grade.film.dbcollect.bean.FilmMsg;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Created with IntelliJ IDEA.
 * User: xiaowu lei
 * Date: 13-12-3
 * Time: 下午3:20
 */
public class HbaseLoad extends AbstractDB implements Callable<List<FilmMsg>> {

    public HbaseLoad(String[] cause, String key) {
        super(cause, key);
    }

    @Override
    public List<FilmMsg> getCache(final String key) {
        return Redis.execute(new IRedisCache<List<FilmMsg>>() {
            @Override
            public List<FilmMsg> callBack(Jedis jedis) throws CacheExecption {
                if (jedis.exists(key)) {
                    String json = jedis.get(key);
                    return JSONArray.parseArray(json, FilmMsg.class);
                }
                return null;
            }
        });
    }

    @Override
    public List<FilmMsg> getDB(String[] cause) {
        return HbaseUtils.callback(new IHbaseCallBack<List<FilmMsg>>() {
            @Override
            public List<FilmMsg> callback(HTableInterface hTable, String key) {
                try {
                    Result results =hTable.get(new Get(Bytes.toBytes(key)));
                    String value=Bytes.toString(results.getValue(Bytes.toBytes("base"), Bytes.toBytes("msg")));
                    if(Strings.isNullOrEmpty(value)){
                        return null;
                    }
                    return JSON.parseObject(value,new TypeReference<List<FilmMsg>>(){});
                } catch (IOException e) {
                    System.out.print(e.getLocalizedMessage());
                }
                return null;
            }
        },cause[0],cause[1]);
    }

    @Override
    public List<FilmMsg> call() throws Exception {
        return get();
    }
    public static void main(String[]args){
        Configuration configuration = HBaseConfiguration.create();
        configuration.addResource("grade.xml");

        HbaseLoad load=new HbaseLoad(new String[]{"/media/new/2013/10/15/hd_zy_sk02_20131015.ts",configuration.get("film.base.msg")},"/media/new/2013/10/15/hd_zy_sk02_20131015.ts");
        DBLoadThread loadThread=DBLoadThread.getInstance();
        List<FilmMsg> list=load.getDB(new String[]{"/media/new/2013/10/15/hd_zy_sk02_20131015.ts",configuration.get("film.base.msg")}) ;
        System.out.print(JSON.toJSONString(list));
    }
}
