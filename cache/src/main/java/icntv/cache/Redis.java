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

package icntv.cache;

import icntv.pro.PropertiesUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

/**
 * Created with IntelliJ IDEA.
 * User: xiaowu lei
 * Date: 13-11-15
 * Time: 下午1:32
 */
public class Redis {

    private static final String REDIS_IP="redis.ip";
    private static final String REDIS_PORT="redis.port";
    private static final String REDIS_PWD="redis.pwd";
    private static final String REDIS_MAXACTIVE="redis.maxactive";
    private static final String REDIS_MAXWAIT="redis.maxwait";
    private static final String REDIS_DB="redis.db";
    private Redis(){

    }
    private static PropertiesUtils propertiesUtils=PropertiesUtils.getInstance();

    private static JedisPool jedisPool;
    private static synchronized void init(){
        if(null == jedisPool){
            JedisPoolConfig config=new JedisPoolConfig();
            config.setMaxActive(propertiesUtils.getInt(REDIS_MAXACTIVE));
            config.setMaxWait(propertiesUtils.getInt(REDIS_MAXWAIT));
            config.setTestOnBorrow(true);
            jedisPool=new JedisPool(config,
                    propertiesUtils.getString(REDIS_IP),
                    propertiesUtils.getInt(REDIS_PORT),
                    Protocol.DEFAULT_TIMEOUT,
                    propertiesUtils.getString(REDIS_PWD));
        }
    }

    public static Jedis getJedis(){
        init();
        Jedis jedis= jedisPool.getResource();
        jedis.select(propertiesUtils.getInt(REDIS_DB));
        return jedis;
    }

    public static void returnResource(Jedis jedis){
        if(null != jedis){
            jedisPool.returnResource(jedis);
        }
    }

    public static  <T> T execute(IRedisCache<T> cache){
        Jedis jedis=null;
        try{
            jedis= getJedis();
            jedis.select(propertiesUtils.getInt(REDIS_DB));
            return cache.callBack(jedis);
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }finally {
           returnResource(jedis);
        }
    }
    public static void main(String[]args){

    }
}
