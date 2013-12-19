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

package tv.icntv.grade.film;

import org.apache.hadoop.util.ProgramDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tv.icntv.grade.film.dbcollect.DBCollectJob;
import tv.icntv.grade.film.dbcollect.TableConcurrencyJob;
import tv.icntv.grade.film.dbcollect.TableInitJob;
import tv.icntv.grade.film.grade.GradeJob;
import tv.icntv.grade.film.recommend.CFRecommendJob;
import tv.icntv.grade.film.recommend.CorrelateJob;
import tv.icntv.grade.film.recommend.CorrelateResultJob;
import tv.icntv.grade.film.recommend.TopNJob;

/**
 * Created with IntelliJ IDEA.
 * User: xiaowu lei
 * Date: 13-11-27
 * Time: 下午1:59
 */
public class Driver {
    public static void main(String[]args){
        int exitCode=1;
        ProgramDriver pd = new ProgramDriver();
        try{
            pd.addClass("grade",Grade.class,"icntv grade composite of init,calculate,recommend");
            pd.addClass("init", TableInitJob.class,"init job, collect db");
            pd.addClass("db-init-new", TableConcurrencyJob.class,"concurrency job");
//            pd.addClass("calculate", GradeJob.class,"calculate grade");
            pd.addClass("cf-recommend-u", CFRecommendJob.class,"personality recommend");
            pd.addClass("correlate-u", CorrelateJob.class,"correlate recommend");
            pd.addClass("local-correlate", CorrelateResultJob.class,"local correlate");
            pd.addClass("topN", TopNJob.class,"calculate top n");
            pd.addClass("dbcollect", DBCollectJob.class,"read from hdfs and hbase unit data;");
            pd.driver(args);
            exitCode=0;
        }catch (Exception e){
            e.printStackTrace();
        } catch (Throwable throwable) {
            throwable.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        System.exit(exitCode);
    }
}
