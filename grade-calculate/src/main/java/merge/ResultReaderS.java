package merge;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.mahout.common.Pair;
import org.apache.mahout.fpm.pfpgrowth.convertors.string.TopKStringPatterns;

public class ResultReaderS {
    public static Map<Integer, Long> readFrequency(Configuration configuration, String fileName) throws Exception {
        FileSystem fs = FileSystem.get(configuration);
        Reader frequencyReader = new SequenceFile.Reader(fs,
                new Path(fileName), configuration);
        Map<Integer, Long> frequency = new HashMap<Integer, Long>();
        Text key = new Text();
        LongWritable value = new LongWritable();
        while(frequencyReader.next(key, value)) {
            frequency.put(Integer.parseInt(key.toString()), value.get());
        }
        return frequency;
    }
 
 
    public static void readFrequentPatterns(
            Configuration configuration,
            String fileName,
            int transactionCount,
            Map<Integer, Long> frequency,
            double minSupport, double minConfidence) throws Exception {
        FileSystem fs = FileSystem.get(configuration);
 
        Reader frequentPatternsReader = new SequenceFile.Reader(fs,
                new Path(fileName), configuration);
        Text key = new Text();
        TopKStringPatterns value = new TopKStringPatterns();
 
        while(frequentPatternsReader.next(key, value)) {
            long firstFrequencyItem = -1;
            String firstItemId = null;
            List<Pair<List<String>, Long>> patterns = value.getPatterns();
            int i = 0;
            for(Pair<List<String>, Long> pair: patterns) {
                List<String> itemList = pair.getFirst();
                Long occurrence = pair.getSecond();
                if (i == 0) {
                    firstFrequencyItem = occurrence;
                    firstItemId = itemList.get(0);
                } else {
                    double support = (double)occurrence / transactionCount;
                    double confidence = (double)occurrence / firstFrequencyItem;
                    if ((support > minSupport
                            && confidence > minConfidence)) {
                        List<String> listWithoutFirstItem = new ArrayList<String>();
                        for(String itemId: itemList) {
                            if (!itemId.equals(firstItemId)) {
                                
                                listWithoutFirstItem.add(itemId);
                            }
                        }
                        String firstItem = firstItemId;
                        listWithoutFirstItem.remove(firstItemId);
                        System.out.printf(
                            "%s => %s: supp=%.3f, conf=%.3f",
                            listWithoutFirstItem,
                            firstItem,
                            support,
                            confidence);
 
                        if (itemList.size() == 2) {
                            // we can easily compute the lift and the conviction for set of
                            // size 2, so do it
                            int otherItemId = -1;
                            for(String itemId: itemList) {
                                if (!itemId.equals(firstItemId)) {
                                    otherItemId = Integer.parseInt(itemId);
                                    break;
                                }
                            }
                            long otherItemOccurrence = frequency.get(otherItemId);
                            double lift = (double)occurrence / (firstFrequencyItem * otherItemOccurrence);
                            double conviction = (1.0 - (double)otherItemOccurrence / transactionCount) / (1.0 - confidence);
                            System.out.printf(
                                ", lift=%.3f, conviction=%.3f",
                                lift, conviction);
                        }
                        System.out.printf("\n");
                    }
                }
                i++;
            }
        }
        frequentPatternsReader.close();
 
    }
 
    public static void main(String args[]) throws Exception {
 
        int transactionCount = 88162;//事务总数
        String frequencyFilename = "data/fList.seq";//
        String frequentPatternsFilename = "data/frequentpatterns.seq";
        double minSupport = 0.001;//支持度
        double minConfidence = 0.3;//置信度
 
 
        Configuration configuration = new Configuration();
        Map<Integer, Long> frequency = readFrequency(configuration, frequencyFilename);
        readFrequentPatterns(configuration, frequentPatternsFilename,
                transactionCount, frequency, minSupport, minConfidence);
 
    }
}