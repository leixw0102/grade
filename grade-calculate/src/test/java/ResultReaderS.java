import java.util.*;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.mahout.common.Pair;
import org.apache.mahout.fpm.pfpgrowth.convertors.string.TopKStringPatterns;
import tv.icntv.grade.film.utils.HadoopUtils;

public class ResultReaderS {
    public static Map<Integer, Long> readFrequency(Configuration configuration, String fileName) throws Exception {
        FileSystem fs = FileSystem.get(configuration);
        Reader frequencyReader = new SequenceFile.Reader(fs,
                new Path(fileName), configuration);
        Map<Integer, Long> frequency = new HashMap<Integer, Long>();
        Text key = new Text();
        LongWritable value = new LongWritable();
        while (frequencyReader.next(key, value)) {
            frequency.put(Integer.parseInt(key.toString()), value.get());
        }
        return frequency;
    }


    public static void readFrequentPatterns(
            Configuration configuration,
            String fileName,
            int transactionCount,
//            Map<Integer, Long> frequency,
            double minSupport, double minConfidence) throws Exception {
        FileSystem fs = FileSystem.get(configuration);
        FileStatus[] ff = fs.listStatus(new Path(fileName), new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return path.getName().matches("part-r-\\d*");
            }
        });
        Map<String,Set<String>>  maps = Maps.newHashMap();
        int count=0;
        for (FileStatus f : ff) {
            Reader frequentPatternsReader = new SequenceFile.Reader(fs,
                    f.getPath(), configuration);
            Text key = new Text();
            TopKStringPatterns value = new TopKStringPatterns();

            while (frequentPatternsReader.next(key, value)) {
                count++;
                long firstFrequencyItem = -1;
                String firstItemId = null;
                List<Pair<List<String>, Long>> patterns = value.getPatterns();
                int i = 0;
                for (Pair<List<String>, Long> pair : patterns) {
                    List<String> itemList = pair.getFirst();
                    Long occurrence = pair.getSecond();
//                    System.out.println(itemList.toString()+"\t"+occurrence);
                    if (i == 0) {
                        firstFrequencyItem = occurrence;
                        firstItemId = itemList.get(0);
                        if(null == firstItemId){
                            continue;
                        }
                    } else {
                        double support = (double) occurrence / transactionCount;
                        double confidence = (double) occurrence / firstFrequencyItem;
                        if ((support > minSupport
                                && confidence > minConfidence)) {
                            List<String> listWithoutFirstItem = new ArrayList<String>();
                            if(null == firstItemId){
                                continue;
                            }
                            for (String itemId : itemList) {
                                if (!itemId.equals(firstItemId) && !Strings.isNullOrEmpty(itemId)) {
//                                      if(maps.containsKey(itemId)){
//                                          Set<String> vs=maps.get(itemId);
//                                          vs.add(firstItemId);
//                                      }else{
//                                          maps.put(itemId, Sets.newHashSet(firstItemId));
//
//                                      }
//                                    listWithoutFirstItem.add(itemId);
                                    System.out.println(itemId+"\t"+firstItemId+"\t"+support +"\t"+f.getPath().toString());
                                }
                            }
//                            String firstItem = firstItemId;
//                            listWithoutFirstItem.remove(firstItemId);
//                            System.out.printf(
//                                    "%s => %s: supp=%.8f, conf=%.3f",
//                                    listWithoutFirstItem,
//                                    firstItem,
//                                    support,
//                                    confidence);

//                            if (itemList.size() == 2) {
//                                // we can easily compute the lift and the conviction for set of
//                                // size 2, so do it
//                                int otherItemId = -1;
//                                for (String itemId : itemList) {
//                                    if (!itemId.equals(firstItemId)) {
//                                        otherItemId = Integer.parseInt(itemId);
//                                        break;
//                                    }
//                                }
//                                long otherItemOccurrence = frequency.get(otherItemId);
//                                double lift = (double) occurrence / (firstFrequencyItem * otherItemOccurrence);
//                                double conviction = (1.0 - (double) otherItemOccurrence / transactionCount) / (1.0 - confidence);
//                                System.out.printf(
//                                        ", lift=%.8f, conviction=%.3f",
//                                        lift, conviction);
//                            }
//                            System.out.printf("\n");
                        }
                    }
                    i++;
                }
            }
            frequentPatternsReader.close();
        }
//        Set<String> keys =maps.keySet();
//        for(String k: keys) {
//            System.out.println(k + "\t" + maps.get(k));
//        }
        System.out.println(count);
    }

    public static void main(String args[]) throws Exception {

        int transactionCount = 88162;//事务总数
        String frequencyFilename = "/test/1233/fList";//
        String frequentPatternsFilename = "/icntv/grade/correlate-fp-growth/2013-12-12/frequentpatterns";
        double minSupport = 0.006;//支持度
        double minConfidence = 0.03;//置信度

        long count= HadoopUtils.count(new Path(frequentPatternsFilename), new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return path.getName().matches("part-r-\\d*");  //To change body of implemented methods use File | Settings | File Templates.
            }
        });
        Configuration configuration = new Configuration();
//        Map<Integer, Long> frequency = readFrequency(configuration, frequencyFilename);
        readFrequentPatterns(configuration, frequentPatternsFilename,
                (int) count, minSupport, minConfidence);
//        System.out.println(count);

    }
}