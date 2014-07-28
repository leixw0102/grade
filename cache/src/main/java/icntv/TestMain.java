package icntv;/*
 * Copyright 2014 Future TV, Inc.
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

import icntv.cache.IRedisCache;
import icntv.cache.Redis;
import icntv.exception.CacheExecption;
import redis.clients.jedis.Jedis;

/**
 * Created by leixw
 * <p/>
 * Author: leixw
 * Date: 2014/02/25
 * Time: 12:42
 */
public class TestMain {
    public static void main(String[]args){
        Redis.execute(new IRedisCache<Object>() {
            @Override
            public Object callBack(Jedis jedis) throws CacheExecption {
                jedis.set("abc","efg");
                System.out.println(jedis.get("abc"));
                return null;  //To change body of implemented methods use File | Settings | File Templates.
            }
        });
    }
}
