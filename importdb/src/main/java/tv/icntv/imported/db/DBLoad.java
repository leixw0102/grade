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

package tv.icntv.imported.db;

import com.alibaba.fastjson.JSONArray;
import com.google.common.collect.Lists;
import icntv.cache.IRedisCache;
import icntv.cache.Redis;
import icntv.exception.CacheExecption;
import redis.clients.jedis.Jedis;
import tv.icntv.imported.db.bean.FilmMsg;
import tv.icntv.imported.db.bean.Films;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Created with IntelliJ IDEA.
 * User: xiaowu lei
 * Date: 13-12-3
 * Time: 上午9:13
 */
public class DBLoad extends AbstractDB implements Callable<List<FilmMsg>> {
    private final String FIND_TIME_AND_PROGRAMID = "select a.PROGRAM_ID,a.PLAY_URL,b.PROGRAM_LENGTH,b.program_name,b.zone,b.years,b.director,d.leading_role,d.tag,d.PROGRAM_CLASS," +
            "d.SCRIPT_WRITER,d.PROGRAM_SERIES_ID " +
            "from cms_program b,CMS_PROGRAM_SERIES_REL c,CMS_PROGRAM_SERIES d," +
            " (select program_id,play_url from CMS_PROGRAM_BITRATE where PLAY_URL %s) a " +
            "  where  a.program_id = b.program_id and a.PROGRAM_ID = c. PROGRAM_ID and c.PROGRAM_SERIES_ID = d.PROGRAM_SERIES_ID ";

    public DBLoad(String[] cause, String key) {
        super(cause, key);
    }

    @Override
    public List<FilmMsg> call() throws Exception {
        return get();
    }

    @Override
    public List<FilmMsg> getCache(final String key) {
        return Redis.execute(new IRedisCache<List<FilmMsg>>() {
            @Override
            public List<FilmMsg> callBack(Jedis jedis) throws CacheExecption {
                if (jedis.exists(key)) {
                    String json = jedis.get(key);
                    return JSONArray.parseArray(json, FilmMsg.class);
                }
                return null;
            }
        });
    }

    @Override
    public List<FilmMsg> getDB(final String[] cause) {
        return DBUtils.callback(new IDbCallBack<List<FilmMsg>>() {
            @Override
            public List<FilmMsg> callback(Connection connection) {
                String url = cause[0];
                final String temp = url.startsWith("http://") || url.startsWith("https://") || url.startsWith("www.") ? " = '" + url + "'" : "like '%" + url + "'";
                String sql = String.format(FIND_TIME_AND_PROGRAMID, temp);
                List<FilmMsg> filmMsgs = Lists.newArrayList();
                PreparedStatement pst = null;

                ResultSet rs = null;
                try {
                    pst = connection.prepareStatement(sql);
                    rs = pst.executeQuery();
                    while (rs.next()) {
                        filmMsgs.add(new FilmMsg.FileMsgBuilder()
                                .setId(rs.getLong(1))
                                .setUrl(rs.getString(2))
                                .setTime(rs.getLong(3))
                                .setName(rs.getString(4))
                                .setZone(rs.getString(5))
                                .setYear(rs.getString(6))
                                .setDirector(rs.getString(7))
                                .setActors(rs.getString(8))
                                .setTag(rs.getString(9))
                                .setCategory(rs.getString(10))
                                .setWriter(rs.getString(11))
                                .setProgramId(rs.getLong(12))
                                .builder());
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    if (null != rs) {
                        try {
                            rs.close();
                        } catch (SQLException e) {
                            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                        }
                    }
                    if (null != pst) {
                        try {
                            pst.close();
                        } catch (SQLException e) {
                            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                        }
                    }
                }
                return filmMsgs;
            }
        });
    }
    public static void main(String[]args){
        List<FilmMsg> list= new DBLoad(new String[]{},"").getDB(new String[]{"/media/new/2013/09/11/hd_dy_ylgz_20130911.ts"}) ;
        System.out.println(Films.toString(list));
    }
}
