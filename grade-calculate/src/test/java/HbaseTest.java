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

import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: xiaowu lei
 * Date: 13-11-27
 * Time: 上午9:20
 */
public class HbaseTest {
    public static void main(String[]args) throws IOException {
//        HbaseUtils hbaseUtils=HbaseUtils.getHbaseUtils(HBaseConfiguration.create());
//        HTable hTable=hbaseUtils.getHtable("icntv.grade.film.num");
//        Result result=hTable.get(new Get(Bytes.toBytes("com.ysten.media.hot:http/media/new/2012/11/30/sd_zy_xcm05_20121130.ts")));
//
//        System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("base"),Bytes.toBytes("url"))));
//        System.out.println(Long.parseLong(new String(result.getValue(Bytes.toBytes("base"),Bytes.toBytes("num")))));
//        hbaseUtils.release(hTable);
        byte[] x=Bytes.toBytes("20");

        System.out.println(Bytes.toString(x));
    }
}
