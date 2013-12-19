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
//import com.google.common.base.Strings;
//import com.google.common.primitives.Doubles;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hbase.client.Result;
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
//import org.apache.hadoop.hbase.mapreduce.TableMapper;
//import org.apache.hadoop.hbase.util.Bytes;
//import org.apache.hadoop.io.DoubleWritable;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Mapper;
//
//import java.io.IOException;
//
///**
// * Created with IntelliJ IDEA.
// * User: xiaowu lei
// * Date: 13-11-27
// * Time: 上午11:14
// */
//public class GradeMapper extends Mapper<LongWritable,Text,Text,Text> {
//    private final String TIME_PERCENT="grade.element.time.percent";
//
//    private double timePercentValue=0.0;
//
//    private final String FAMILY="base";
//    private final String timeQualifier="timeScore" ;
//    @Override
//    protected void setup(Context context) throws IOException, InterruptedException {
//        Configuration configuration = context.getConfiguration();
//        timePercentValue= Strings.isNullOrEmpty(configuration.get(TIME_PERCENT))?1.0: Doubles.tryParse(configuration.get(TIME_PERCENT));
//
//    }
//
//    @Override
//    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        if(null == value || Strings.isNullOrEmpty(value.toString())){
//            return ;
//        }
//        Double timeScore=Doubles.tryParse(Bytes.toString(value.));
//        context.write(key,new Text(timeScore*timePercentValue+""));
//
//    }
////    @Override
////    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
////        Double timeScore=Doubles.tryParse(Bytes.toString(value.getValue(Bytes.toBytes(FAMILY),Bytes.toBytes(timeQualifier))));
////        context.write(key,new Text(timeScore*timePercentValue+""));
////    }
//}
