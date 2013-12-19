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

package tv.icntv.grade.film.utils;

import com.google.common.io.Closeables;
import com.google.common.io.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Closeable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.common.HadoopUtil;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;


/**
 * Created with IntelliJ IDEA.
 * User: xiaowu lei
 * Date: 13-12-5
 * Time: 上午9:04
 */
public class HadoopUtils {
    private static Configuration configuration = new Configuration();
    public static boolean isExist(Path path) throws IOException {
        FileSystem fileSystem=null;
        try {
            fileSystem=FileSystem.get(configuration);
            return fileSystem.exists(path);
        } catch ( Exception e){
            return false;
        }finally {
            if(null != fileSystem){
                fileSystem.close();
            }
        }
    }

    public static void deleteIfExist(String path) throws IOException {
        FileSystem fileSystem=null;
        try {
            fileSystem=FileSystem.get(configuration);
            Path p = new Path(path);
            if(fileSystem.exists(p)){
                fileSystem.delete(p,true);
            }
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } finally {
            if(null != fileSystem){
                fileSystem.close();
            }
        }

    }
    public static boolean isExist(Path ...paths) throws IOException {
        if(null == paths || paths.length==0){
            return false;
        }
        for(Path path : paths){
            if(!isExist(path)){
                return false;
            }
        }
        return true;
    }

    public static void save(Path path,Long size) throws IOException {
        FileSystem fileSystem=null;
        FSDataOutputStream out=null;
        try {

            fileSystem=FileSystem.get(configuration);
            if(fileSystem.exists(path)){
                fileSystem.delete(path);
                out= fileSystem.create(path);
                out.writeLong(size);
                out.flush();
            }

        }catch (Exception e){
            return ;
        } finally {
            Closeables.close(out,true);
            if(null != fileSystem){
                fileSystem.close();
            }
        }
    }

    public static long count(Path path,PathFilter filter) throws Exception {
        FileSystem fileSystem=null;

        try{
            fileSystem=FileSystem.get(configuration);
           FileStatus[] fs =fileSystem.listStatus(path,filter);
            long count=0;
            for(FileStatus f : fs){
                SequenceFile.Reader frequentPatternsReader = new SequenceFile.Reader(fileSystem,
                        f.getPath(), configuration);
                Text key = new Text();
                while (frequentPatternsReader.next(key)){
                    count++;
                }
               frequentPatternsReader.close();
            }
            return count;
        }catch (Exception e){
            throw e;
        }finally {
//            Closeables.close(br,true);
            if(null != fileSystem){
                fileSystem.close();
            }
        }
    }

    public static void main(String[]args) throws Exception {
        Configuration configuration=new Configuration();
//        deleteIfExist("/icntv/grade/db/2013-12-12/chinacache_table_1");
//       long c= count(new Path("/icntv/grade/correlate-fp-growth/2013-12-10/frequentpatterns"),new PathFilter() {
//            @Override
//            public boolean accept(Path path) {
//
//                return path.getName().matches("part-r-\\d*");
//            }
//        });
//        System.out.println(c);
//        String line="shut";
//        System.out.println(line.matches("\\d*"));
        FileSystem fileSystem = FileSystem.get(configuration);
        BufferedReader b = new BufferedReader(new InputStreamReader(fileSystem.open(new Path("/icntv/grade/num/middle/2013-12-18/part-r-00000"))));
        String line = null;
        while(null!= (line=b.readLine())){
            System.out.println(line.split("\t").length +" --- "+line);
        }
        b.close();


//        FileStatus[] fs=fileSystem.listStatus(new Path("/icntv/grade/score/base/2013-12-09"),new PathFilter() {
//            @Override
//            public boolean accept(Path path) {
//                return path.getName().matches("part-r-\\d*");
//            }
//        });
//        BufferedReader br =null;
//        String line="shoutcastsource";
//        for(FileStatus f:fs){
////            fileSystem.open(f.getPath())
//            br = new BufferedReader( new InputStreamReader(fileSystem.open(f.getPath())));
//            String l=null;
//            while (null!=(l=br.readLine())){
//                if(l.contains(line)){
//                    System.out.println(l);
//                }
//            }
//        }
//        Closeables.close(br,true);
//        if(null != fileSystem){
//            fileSystem.close();
//        }
    }

}
