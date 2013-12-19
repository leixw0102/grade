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

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;

/**
 * Created with IntelliJ IDEA.
 * User: xiaowu lei
 * Date: 13-11-18
 * Time: 下午6:07
 */
public class TableInitMapper extends org.apache.hadoop.hbase.mapreduce.TableMapper<Text,Text> {
    private static final String columnFamily = "base";
    private static final String startTime = "begin_time"; //开始时间
    private static final String endTime = "end_time"; //结束时间
    private static final String bytes="bytes";//流量
    private Logger logger = LoggerFactory.getLogger(getClass());
    private static SimpleDateFormat format = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss Z", Locale.UK);


    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        if(null == key){
            return;
        }
        String strKey=Bytes.toString(key.get());
        strKey=strKey.substring(0,strKey.length()-15);
        if(strKey.length()<=15){
            return;
        }
        String temp=strKey.substring(0,15);
        if(!temp.matches("\\d*")){
            return;
        }
        String time= Bytes.toString(value.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(startTime)));
        long start=getLong(time);
        time = Bytes.toString(value.getValue(Bytes.toBytes(columnFamily),Bytes.toBytes(endTime)));
        long end=getLong(time);
        String  flow=Bytes.toString(value.getValue(Bytes.toBytes(columnFamily),Bytes.toBytes(bytes)));
        context.progress();
        context.write(new Text(strKey),new Text((end-start)+"\t"+flow));

    }

    protected long getLong(String input){
        try {
            return (null==input||input.equals("") )?0:format.parse(input).getTime();
        } catch (ParseException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            return -1L;
        }
    }

}
