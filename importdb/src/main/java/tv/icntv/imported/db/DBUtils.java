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

import com.jolbox.bonecp.BoneCP;
import com.jolbox.bonecp.BoneCPConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tv.icntv.imported.utils.PropertiesUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * User: xiaowu lei
 * Date: 13-11-20
 * Time: 下午2:36
 */
public class DBUtils {
    private static PropertiesUtils propertiesUtils = PropertiesUtils.getInstance("jdbc.properties");

    private static final String CLASS_NAME = "jdbc.driver.class";
    private static final String JDBC_URL="jdbc.url";
    private static final String JDBC_USER="jdbc.user";
    private static final String JDBC_PWD="jdbc.pwd";
    private static final String JDBC_IDLE_MAX="jdbc.bonecp.idleMaxAge";
    private static final String JDBC_IDLE_CONNECTION_TEST_PERIOD="jdbc.bonecp.idleConnectionTestPeriod";
    private static final String JDBC_PARTITION_COUNT="jdbc.bonecp.partitionCount";
    private static final String JDBC_ACQUIRE_INCREMENT="jdbc.bonecp.acquireIncrement";
    private static final String JDBC_MAX_CONNECTION_PER_PARTITION="jdbc.bonecp.maxConnectionsPerPartition" ;
    private static final String JDBC_MIN_CONNECTION_PER_PARTITION="jdbc.bonecp.minConnectionsPerPartition";
    private static BoneCP boneCP = null;

   static {
        try {
            // load the database driver (make sure this is in your classpath!)
            Class.forName(propertiesUtils.getString(CLASS_NAME));
        } catch (Exception e) {
            System.out.println("error");
        }
        BoneCPConfig config=new BoneCPConfig();
        config.setJdbcUrl(propertiesUtils.getString(JDBC_URL));
        config.setUsername(propertiesUtils.getString(JDBC_USER));
        config.setPassword(propertiesUtils.getString(JDBC_PWD));
        config.setIdleMaxAge(propertiesUtils.getInt(JDBC_IDLE_MAX), TimeUnit.MINUTES);
        config.setIdleConnectionTestPeriod(propertiesUtils.getInt(JDBC_IDLE_CONNECTION_TEST_PERIOD), TimeUnit.MINUTES);
        config.setPartitionCount(propertiesUtils.getInt(JDBC_PARTITION_COUNT));
        config.setAcquireIncrement(propertiesUtils.getInt(JDBC_ACQUIRE_INCREMENT));
        config.setMaxConnectionsPerPartition(propertiesUtils.getInt(JDBC_MAX_CONNECTION_PER_PARTITION));
        config.setMinConnectionsPerPartition(propertiesUtils.getInt(JDBC_MIN_CONNECTION_PER_PARTITION));
        config.setConnectionTimeout(1,TimeUnit.HOURS);
        try {
            boneCP=new BoneCP(config);
            Connection connection=boneCP.getConnection();
            connection.close();
            connection=null;
        } catch (SQLException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }
//
//    public  static DBUtils getInstance() {
//       return new DBUtils();
//    }

    private DBUtils() {
    }

    public synchronized static Connection getConnection() {

        try {
            return boneCP.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

        return null;
    }

    public synchronized static void close(Connection connection) {

        try {
            if (null != connection && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    private Logger logger = LoggerFactory.getLogger(getClass());

    public static  <T> T callback(IDbCallBack<T> callBack) {
        Connection connection = null;
        try {
            connection = getConnection();
            if (null == connection) {
//                logger.error("create db connection error");
                return null;
            }
            return callBack.callback(connection);
        } catch (Exception e) {
            e.printStackTrace();
//            logger.error("db executor error!", e);
        } finally {
            close(connection);
        }
        return null;
    }
}