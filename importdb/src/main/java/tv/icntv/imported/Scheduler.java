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

package tv.icntv.imported;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.util.Date;

/**
* Created with IntelliJ IDEA.
* User: xiaowu lei
* Date: 13-12-3
* Time: 下午2:19
*/
public class Scheduler {
    public static void main(String[]args) throws SchedulerException {
        new Scheduler().start();
    }

    public void start() throws SchedulerException {
        SchedulerFactory sf = new StdSchedulerFactory();
        org.quartz.Scheduler scheduler=sf.getScheduler();
        JobDetail detail = JobBuilder.newJob(ImportJob.class).withIdentity("import data","icntv").build();
        Trigger trigger= TriggerBuilder.newTrigger().withIdentity("trigger", "icntv").startAt(new Date()).withSchedule(SimpleScheduleBuilder.repeatHourlyForever(12).repeatForever()).build();
        scheduler.scheduleJob(detail,trigger);
        scheduler.start();
    }

}
