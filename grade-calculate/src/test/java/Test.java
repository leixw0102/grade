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

import com.google.common.base.CharMatcher;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.primitives.Longs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import tv.icntv.grade.film.Grade;
import tv.icntv.grade.film.utils.HadoopUtils;

import java.io.IOException;
import java.text.MessageFormat;
import java.text.ParseException;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Created with IntelliJ IDEA.
 * User: xiaowu lei
 * Date: 13-11-19
 * Time: 上午11:50
 */
public class Test {
    public static void main(String[]args) throws Exception {
//        String FIND_TIME_AND_PROGRAMID = "select a.PROGRAM_ID,a.PLAY_URL,b.PROGRAM_LENGTH,b.program_name,b.zone,b.years,b.director,d.leading_role,d.tag,d.PROGRAM_CLASS," +
//                "d.SCRIPT_WRITER,d.PROGRAM_SERIES_ID " +
//                "from cms_program b,CMS_PROGRAM_SERIES_REL c,CMS_PROGRAM_SERIES d," +
//                " (select program_id,play_url from CMS_PROGRAM_BITRATE where PLAY_URL %s) a " +
//                "  where  a.program_id = b.program_id and a.PROGRAM_ID = c. PROGRAM_ID and c.PROGRAM_SERIES_ID = d.PROGRAM_SERIES_ID ";
//        System.out.println(String.format(FIND_TIME_AND_PROGRAMID,"like 'ssss'"));
//        System.out.println(Longs.tryParse(""));
//        System.out.println(123%1000);
//        double x=0.2f;
//        System.out.println(x);
////        .
//        System.out.println("234567890".substring(2,"234567890".length()));
//        String s="010101002154486/media/new/2013/09/07/hd_zy_hsy906_20130907.ts000000000000000";
//        String str="010133501439929/media/new/2012/10/12/sd_zy_lmsezj068_20121012.ts";
//        System.out.println(str.substring(15,str.length()));
//        System.out.println(s.substring(0,s.length()-15));
//        System.out.
//       System.out.println( MessageFormat.format("/temp/cf/recommend/{0}",System.nanoTime()+""));
//        String pattern="http://www.youku.com/show_page/\\w+.html";
//        boolean flag= Pattern.compile(pattern).matcher("http://www.youku.com/show_page/id_z805d2162bceb11e0bf93.html#anchor").find();
//        System.out.println(flag);
//        Configuration configuration=HBaseConfiguration.create();
//        configuration.addResource("grade.xml");
//        String input = String.format(configuration.get("icntv.correlate.input"),new Date());
//        System.out.println(input);
//        long count= HadoopUtils.count(new Path(input),new PathFilter() {
//            @Override
//            public boolean accept(Path path) {
//                return path.getName().matches("part-r-\\d*");  //To change body of implemented methods use File | Settings | File Templates.
//            }
//        });
//        System.out.println(count);
        String t="http://vod01.media.ysten.com/media/new/2011/12/26/hd_dy_xqdzzz1_20111226.ts     1335151,1411025 774727,780211   129";
        System.out.print(Splitter.on("\t").splitToList(t).size());
    }

}
