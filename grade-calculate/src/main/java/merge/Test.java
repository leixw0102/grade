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

package merge;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MetaUtils;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 * User: xiaowu lei
 * Date: 13-11-21
 * Time: 下午1:25
 */
public class Test {
    public static void main(String[]args) throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        MetaUtils metaUtils=new MetaUtils(configuration);

//       List<HRegionInfo> x= metaUtils.getMETARows(Bytes.toBytes("chinacache_table_1"));
//        for(HRegionInfo info:x){
//            System.out.println(info.getRegionId()+"\t"+info.getRegionName()+"\t"+info.getRegionNameAsString());
//        }
//        HbaseUtils hbaseUtils=HbaseUtils.getHbaseUtils(configuration);
//        HBaseAdmin.checkHBaseAvailable(configuration);
//        HBaseAdmin admin= new HBaseAdmin(configuration) ;
//        HMerge.merge(configuration, FileSystem.get(configuration),Bytes.toBytes("chinacache_table_1"));

//        HConnectionManager.deleteConnection(configuration, true);
//        HTable hTable=hbaseUtils.getHtable(".META.");
//        Delete delete = new Delete(Bytes.toBytes("chinacache_table_1,010133501124485/media/new/2013/10/16/hd_dsj_fwfy02_20131016.ts000000000006546,1384933612684.36024bbd4f18c72b514cf632c58f8032."));
//        Delete delete1=new Delete(Bytes.toBytes("chinacache_table_1,010133501119479/media/new/2013/05/09/hd_dy_tlnhnm_20130509.ts000000000005104,1384928210198.c079a2bd36d1ef95c2e177bf1369d9bb."));
//        List<Delete> list = Lists.newArrayList(delete,delete1);
//        hTable.delete(list);
//        ResultScanner resultScanner=hTable.getScanner(new Scan());
//        Iterator<Result> it=  resultScanner.iterator();
//        while(it.hasNext()){
//            Result result = it.next();
//            System.out.println(Bytes.toString(result.getRow()));
//        }
//        hbaseUtils.release(hTable);
    }
}
