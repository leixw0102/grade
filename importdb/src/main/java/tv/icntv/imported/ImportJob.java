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

package tv.icntv.imported;

import com.alibaba.fastjson.JSON;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tv.icntv.imported.db.DBLoad;
import tv.icntv.imported.db.DBLoadThread;
import tv.icntv.imported.db.DBUtils;
import tv.icntv.imported.db.IDbCallBack;
import tv.icntv.imported.db.bean.FilmMsg;
import tv.icntv.imported.db.bean.Page;
import tv.icntv.imported.hbase.HbaseUtils;
import tv.icntv.imported.hbase.IHbaseCallBack;

import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: xiaowu lei
 * Date: 13-12-3
 * Time: 下午2:25
 */
public class ImportJob implements Job {
    DBLoadThread loadThread=DBLoadThread.getInstance();
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        int total=getTotal();
        Page page =new Page(total);
        logger.info(total+"\t"+page.getTotalPage());
        for (int i=0;i<=page.getTotalPage();i++){
            List<Put> puts = Lists.newArrayList();

            List<String> urls = getTotalUrls(i*page.getSize(),page.getSize());
//            logger.info("limit "+i*page.getSize()+"\t"+page.getSize());
            for(String url: urls){
                DBLoad load= new DBLoad(new String[]{url},url);
                List<FilmMsg> list=load.get();
                if(null == list || list.isEmpty()){
                    continue;
                }
                if(load.isCache()){
                    continue;
                }
                URL u=null;
                try {
                     u= new URL(url);
                } catch (MalformedURLException e) {
                   continue;
                }
                Put put =new Put(Bytes.toBytes(url));
                put.add(Bytes.toBytes("base"),Bytes.toBytes("msg"),Bytes.toBytes(JSON.toJSONString(list)));
                Put put1 = new Put(Bytes.toBytes(u.getPath()));
                put1.add(Bytes.toBytes("base"),Bytes.toBytes("msg"),Bytes.toBytes(JSON.toJSONString(list)));
                puts.add(put);
                puts.add(put1);
//                logger.info("construct put");
            }
            HbaseUtils.save(puts,"icntv.grade.base.msg");
            logger.info("save to hbase :size="+puts.size());
        }
        logger.info("end...");
    }
    public static void main(String[]args) throws JobExecutionException {
          int i=new ImportJob().getTotal();
        System.out.println(i);
    }

    public List<String> getTotalUrls(final int start,final int size){
        return DBUtils.callback(new IDbCallBack<List<String>>() {
            @Override
            public List<String> callback(Connection connection) {
                PreparedStatement pst=null;
                try {
                    List<String> urls = Lists.newArrayList();
                    logger.info("select play_url  from CMS_PROGRAM_BITRATE  group by play_url limit "+start+","+size);
                    pst= connection.prepareStatement("select play_url  from CMS_PROGRAM_BITRATE  group by play_url limit "+start+","+size);
                    ResultSet set=pst.executeQuery();//select play_url  from CMS_PROGRAM_BITRATE  group by play_url limit "+start+","+size
                    while(set.next()){
                        String url= set.getString(1);
                        if(Strings.isNullOrEmpty(url)){
                            continue;
                        }
                        urls.add(url);
                    }
                    return urls;
                } catch (SQLException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
                return null;
            } ;
        });
    }
        public Integer getTotal(){
            return DBUtils.callback(new IDbCallBack<Integer>() {
                @Override
                public Integer callback(Connection connection) {
                    PreparedStatement pst=null;
                    try {
                        pst= connection.prepareStatement("select count(*)  from CMS_PROGRAM_BITRATE");
                        ResultSet set=pst.executeQuery();
                        if(set.next()){
                           return set.getInt(1);
                        }
                    } catch (SQLException e) {
                        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                    }
                    return 0;
                }
            });
    }
}
