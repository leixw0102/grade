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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.TypeReference;
import com.google.common.base.Splitter;
import com.google.common.collect.*;
import tv.icntv.grade.film.dbcollect.bean.FilmMsg;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: xiaowu lei
 * Date: 13-11-22
 * Time: 下午3:10
 */
public class JsonList {
    public static void main(String[]args){
//        List<FilmMsg> filmMsgList= Lists.newArrayList();
//        filmMsgList.add(new FilmMsg(1));
//        filmMsgList.add(new FilmMsg(2));
//        String json=JSON.toJSONString(filmMsgList);
//        System.out.println(json);
//        List<FilmMsg> x=  JSONArray.parseArray(json, FilmMsg.class);
//        System.out.println(x.size());
//        for(FilmMsg m:x){
//            System.out.println(m.toString());
//        }
//        String ss="0 `1  ` 2     `3    ` 4  ` 5    `6   `  7      `   8  ` 9  `10     `  11`    12";
//        System.out.println(ss.split("`").length);
        List<Map<Long,List<Long>>> x=Lists.newArrayList();
        Map<Long,Set<Long>> maps = Maps.newHashMap();
        maps.put(1L, Sets.newHashSet(4L, 5L));
        maps.put(2L,Sets.newHashSet(6L,5L));
        Map<Long,Set<Long>> maps1 = Maps.newHashMap();
        maps1.put(1L,Sets.newHashSet(3L,5L));
        maps1.put(2L,Sets.newHashSet(6L,5L));
        maps1.put(3L,Sets.newHashSet(6L,5L));
        Map<Long,Set<Long>>  mmm= Maps.newHashMap();

        Set<Long> ks = maps.keySet();
        for(Long k:ks){
            if(mmm.containsKey(k)){
                Set<Long> mss=mmm.get(k);
                mss.addAll(maps.get(k));
                mmm.put(k,mss);
            }else{
                mmm.put(k,maps.get(k));
            }
        }
         ks = maps1.keySet();
        for(Long k:ks){
            if(mmm.containsKey(k)){
                Set<Long> mss=mmm.get(k);
                mss.addAll(maps1.get(k));
                mmm.put(k,mss);
            }else{
                mmm.put(k,maps1.get(k));
            }
        }
        Set<Long> keys = mmm.keySet();

        for(Long l:keys) {
            System.out.println(l+"\t"+mmm.get(l));
        }
       String json=JSON.toJSONString(mmm);
        System.out.println(json);
//       Map<Long,List<Long>> mm=JSON.parseObject(json,new TypeReference<Map<Long,List<Long>>>(){});
//        System.out.println(JSON.toJSONString(mm));
    }
}
