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

import com.google.common.primitives.Floats;
import com.google.common.primitives.Longs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Iterator;
import java.util.regex.Pattern;

/**
 * Created with IntelliJ IDEA.
 * User: xiaowu lei
 * Date: 13-11-6
 * Time: 下午5:46
 */
public class HbaseUtils {
    //    private Configuration configuration = null;
    private static HTablePool hTablePool = null;
    private static int poolSize = 500;
    static {
        hTablePool = new HTablePool(HBaseConfiguration.create(), poolSize);
    }

    private HbaseUtils() {

    }

    public static synchronized   HTableInterface getHtable(String table) {
        HTableInterface hTable = hTablePool.getTable(Bytes.toBytes(table));
        return  hTable;
    }

    public static synchronized   void release(HTableInterface hTable) {
        if (null != hTable) {
            try {
                hTable.flushCommits();
            } catch (IOException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            } finally {
                try {
                    hTable.close();
                } catch (IOException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
            }
        }
    }

    public static synchronized   <T> T callback(IHbaseCallBack<T> callBack,String key,String table){
           HTableInterface hTable=getHtable(table);
            try {
                return callBack.callback(hTable,key);
            }   catch (Exception e){
                System.out.print(e.getLocalizedMessage());
            }   finally {
                release(hTable);
            }
          return null;
    }
    public static void main(String[] args) throws IOException {
//        String url="http://movie.douban.com/subject/3094909/";
//        System.out.println(TableUtil.reverseUrl("http://so.letv.com/film/78222.html"));
//        final HbaseUtils h = HbaseUtils.getHbaseUtils(HBaseConfiguration.create());
//        HTable table = h.getHtable("grade.mysql.test");
////        Get get = new Get("11111".getBytes());
//
//        try {
//            Result result = table.get(new Get("11111".getBytes()));
//            String r1 = Bytes.toString(result.getValue(Bytes.toBytes("base"), Bytes.toBytes("test")));
//            String r2 = Bytes.toString(result.getValue(Bytes.toBytes("base"), Bytes.toBytes("sex")));
//            System.out.println(r1 + "\t" + r2);
//        } catch (IOException e) {
//            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//        } finally {
//            h.release(table);
//        }
//        Pattern p = Pattern.compile("http://t.iqiyi.com/m/[0-9]+");
//        System.out.println(p.matcher("http://t.iqiyi.com/m/1013032").find());
//        callback(new IHbaseCallBack<Object>() {
//            @Override
//            public Object callback(HTable hTable, String key) {
//                try {
//                    Result result=hTable.get(new Get(key.getBytes()));
//                    System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("base"),Bytes.toBytes("msg"))));
//                } catch (IOException e) {
//                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//                }
//                return null;  //To change body of implemented methods use File | Settings | File Templates.
//            }
//        },"/media/new/2013/03/28/sd_dy_xmdy49_20130328.ts","icntv.grade.base.msg") ;
//        HTable hTable=getHtable(".META.");
//        Scan scan=new Scan();
//        scan.setFilter(new KeyOnlyFilter());
//        ResultScanner rs=hTable.getScanner(scan);
//        for(Iterator<Result> it = rs.iterator();it.hasNext();){
//            Result result=it.next();
//
//            System.out.println( Bytes.toString(result.getRow()) );
//        }
//        release(hTable);
//        Delete delete = new Delete("010133501119479/media/new/2013/05/09/hd_dy_tlnhnm_20130509.ts000000000005104,1384928210198.c079a2bd36d1ef95c2e177bf1369d9bb.".getBytes());
//        hTable.delete(delete);
//        release(hTable);
    }
}
