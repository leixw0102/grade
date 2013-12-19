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

package tv.icntv.grade.film.dbcollect.bean;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: xiaowu lei
 * Date: 13-11-22
 * Time: 下午3:11
 * StringBuffer sb=new StringBuffer();
 * sb.append(this.getId()).append("\t").append(this.getUrl()).append("\t").append(this.getTime()*60).append("\t")
 * .append(this.getName()).append("\t").append(this.getTag()).append("\t").append(this.getCategory()).append("\t")
 * .append(this.getYear()).append("\t").append(this.getDirector()).append("\t").append(this.getActors()).append("\t")
 * .append(this.getZone()).append("\t").append(this.getPpvCode()).append("\t").append(this.getWriter());
 * return sb.toString();
 */
public class Films {
    private static String split = "\t";

    public synchronized static String toString(List<FilmMsg> filmMsgs) {
        if(null == filmMsgs || filmMsgs.isEmpty()){
            return null;
        }
        StringBuffer sb = new StringBuffer();
        for (FilmMsg msg : filmMsgs) {
            sb.append(msg.toString()).append(split);
        }
        return sb.substring(0, sb.length() - 1);
    }

    public synchronized static String toString(FilmMsg[] filmMsgs) {
        return toString(Lists.newArrayList(filmMsgs));
    }

}
