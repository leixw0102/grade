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

package icntv.pro;


import java.io.IOException;
import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 * User: xiaowu lei
 * Date: 13-11-15
 * Time: 下午1:33
 */
public class PropertiesUtils {
    private PropertiesUtils(){
       this("redis.properties");
    }
    private PropertiesUtils(String file){
        properties = new Properties();
        try {
            properties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream(file));
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }
    private Properties properties;
    public static PropertiesUtils getInstance() {
       return new PropertiesUtils();
    }
    public static PropertiesUtils getInstance(String file){
       return new PropertiesUtils(file);
    }
    public String getString(String key){
        if(null == properties){
            throw new NullPointerException(" property null");
        }
        return properties.getProperty(key);
    }
    public Integer getInt(String key){
       String value=getString(key);
        if(null == value || "".equals(value)){
            return 0;
        }
        return Integer.parseInt(value);
    }
}
