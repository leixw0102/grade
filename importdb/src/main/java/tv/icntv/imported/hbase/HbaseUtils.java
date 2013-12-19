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

package tv.icntv.imported.hbase;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Iterator;
import java.util.List;

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
//        hTable.setAutoFlush(false);

        return hTable;
    }

    public static synchronized   void release(HTableInterface hTable) {
        if (null != hTable) {
            try {
                hTable.flushCommits();
            } catch (IOException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            } finally {
                try {
//                    hTablePool.putTable(hTable);
                    hTable.close();
                } catch (IOException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
            }
        }
    }

    public static synchronized void save(List<Put> puts,String table){
           HTableInterface hTable=getHtable(table);
        try {
             hTable.put(puts);
        }   catch (Exception e){

        }   finally {
            release(hTable);
        }
    }

    public static synchronized   <T> T callback(IHbaseCallBack<T> callBack,String key,String table){
           HTableInterface hTable=getHtable(table);
            try {
                return callBack.callback(hTable,key);
            }   catch (Exception e){
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
//
//        HTable hTable=getHtable("chinacache_table_1");
//        Scan scan = new Scan();
//        FilterList list = new FilterList();
//        list.addFilter(new PageFilter(100));
//        list.addFilter(new KeyOnlyFilter());
//        RowFilter rowFilter=new RowFilter(CompareFilter.CompareOp.EQUAL,new RegexStringComparator("010101002291445"));
//        list.addFilter(rowFilter);
//
//        scan.setFilter(list);
//        ResultScanner rs = hTable.getScanner(scan);
//        for(Iterator<Result> it = rs.iterator();it.hasNext();){
//            Result result=it.next();
//            System.out.println(Bytes.toString(result.getRow()));
//        }
    }
}
