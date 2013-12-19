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

package tv.icntv.grade.film.dbcollect;

import com.alibaba.fastjson.JSONArray;
import com.google.common.collect.Lists;
import icntv.cache.IRedisCache;
import icntv.cache.Redis;
import icntv.exception.CacheExecption;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tv.icntv.grade.film.dbcollect.bean.FilmMsg;
import tv.icntv.grade.film.dbcollect.bean.Films;
import tv.icntv.grade.film.dbcollect.db.IDbCallBack;
import tv.icntv.grade.film.dbcollect.hbase.HbaseLoad;

import java.io.IOException;

import java.util.List;

/**
* Created with IntelliJ IDEA.
* User: xiaowu lei
* Date: 13-11-21
* Time: 下午5:16
*
*/

public class DefaultHbaseMapper extends Mapper<LongWritable, Text, Text, Text> {

     private Logger logger = LoggerFactory.getLogger(getClass());
     private String table=null;
     DBLoadThread loadThread=null;
    String pattern="\\d*";
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration configuration=context.getConfiguration();
        table=configuration.get("film.base.msg");
        loadThread=DBLoadThread.getInstance();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split("\t");
        if(values[0].length()<=15){
            return;
        }
        String deveiceId= values[0].substring(0,15);
        if(!deveiceId.matches(pattern)){
            return;
        }
        String url = values[0].substring(15, values[0].length());
        List<FilmMsg> filmMsg =loadThread.execute(new HbaseLoad(new String[]{url,table},url));

        if (null == filmMsg||filmMsg.isEmpty()) {
            logger.info("find msg by url {}, key {} null,table {}", url, values[0],table);
            return;
        }
//        logger.info("find key {} successed",values[0]);
        context.progress();
        context.write(new Text(values[0].getBytes()),new Text(values[1]+"\t"+values[2]+"\t"+ Films.toString(filmMsg)));

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

    }
}
