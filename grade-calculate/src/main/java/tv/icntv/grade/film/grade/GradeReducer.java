///* Copyright 2013 Future TV, Inc.
// *
// *      Licensed under the Apache License, Version 2.0 (the "License");
// *      you may not use this file except in compliance with the License.
// *      You may obtain a copy of the License at
// *
// *          http://www.apache.org/licenses/LICENSE-2.0
// *
// *      Unless required by applicable law or agreed to in writing, software
// *      distributed under the License is distributed on an "AS IS" BASIS,
// *      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// *      See the License for the specific language governing permissions and
// *      limitations under the License.
// */
//
//package tv.icntv.grade.film.grade;
//
//import com.google.common.collect.Lists;
//import org.apache.hadoop.hbase.client.Put;
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
//import org.apache.hadoop.hbase.mapreduce.TableReducer;
//import org.apache.hadoop.hbase.util.Bytes;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.Writable;
//import org.apache.hadoop.mapreduce.Reducer;
//
//import java.io.IOException;
//import java.util.List;
//
///**
// * Created with IntelliJ IDEA.
// * User: xiaowu lei
// * Date: 13-11-27
// * Time: 上午11:22
// */
//public class GradeReducer extends TableReducer<ImmutableBytesWritable,Text,ImmutableBytesWritable> {
//    @Override
//    protected void reduce(ImmutableBytesWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//
//        Put put = new Put(key.get());
//        List<Text> list= Lists.newArrayList(values);
//        put.add(Bytes.toBytes("base"),Bytes.toBytes("score"),list.get(0).getBytes());
//        context.write(key,put);
//    }
//}
