package tv.icntv.grade.film.grade.time;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Longs;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import tv.icntv.grade.film.utils.MathExtend;

import java.io.IOException;
import java.util.List;


/**
 * 读取DB，将url转化成节募集id,获取电影时常
 * 010133501439929/media/new/2012/10/12/sd_zy_lmsezj068_20121012.ts{0} \t 观看时常{1} \t 流量{2} \t id`url`time(s)`name
 * `tag`category`year`directory`actors`zone`pptvCode`writer`programId \t ...
 * id`url`time(s)`name`tag`category`year`directory`actors`zone`pptvCode`writer`programId
 * 0 `1  ` 2     `3    ` 4  ` 5    `6   `  7      `   8  ` 9  `10     `  11`    12
 */
public class TimeMaper extends Mapper<LongWritable, Text, Text, Text> {
    String pattern="\\d*";
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if(Strings.isNullOrEmpty(value.toString())){
             return ;
        }
        String text = value.toString();
        List<String> ts= Lists.newArrayList(Splitter.on("\t").split(text));
        if(null == ts || ts.isEmpty()||ts.size()<3){
            return ;
        }
        long seeTimes=null == ts.get(1)?0L: Long.parseLong(ts.get(1));
        if(seeTimes==0L||seeTimes==-1L){
            return;
        }
        String k=ts.get(0).substring(0,15);
        if(!k.matches(pattern)){
            return;
        }
        for(int i=3;i<ts.size();i++){
            String[] filmMsgs=ts.get(i).split("`");
            if(null ==filmMsgs|| filmMsgs.length!=13){
                continue;
            }
            if(null == filmMsgs|| filmMsgs.length==0){
                continue;
            }
            Long time=Long.parseLong(filmMsgs[2]);
            if(time==0L || time ==null){
                continue;
            }
            if(filmMsgs[12]==null){
                continue;
            }
            double x=MathExtend.divide(seeTimes, time * 1000, 3);
            while(x>1){
                x=MathExtend.divide(x,10,3);
            }

            context.progress();
            context.write(new Text(k+filmMsgs[12]),new Text(x+""));

        }
    }

}
