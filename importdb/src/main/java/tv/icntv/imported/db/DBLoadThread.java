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

import java.util.concurrent.*;

/**
 * Created with IntelliJ IDEA.
 * User: xiaowu lei
 * Date: 13-12-3
 * Time: 上午9:35
 */
public class DBLoadThread {
    private static ExecutorService service= Executors.newFixedThreadPool(8);
    private static DBLoadThread dbLoadThread;
    public synchronized static DBLoadThread getInstance(){
       if(null == dbLoadThread ){
           dbLoadThread=new DBLoadThread();
       }
       return dbLoadThread;
    }

    public synchronized  <V> V execute(Callable<V> callable){
        Future<V> future=service.submit(callable);
        try {
            return future.get();
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (ExecutionException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        return null;
    }
}
