package radar.mr;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

/**
 * Custom partitioner: sends every key of one language to its own reducer.
 *
 * The language is the first tab-separated field of the key. The order of
 * languages comes from the job setting "radar.langs" (e.g. "en,hi,ta,ja,pt"),
 * so with 5 reducers part-00000 holds only "en", part-00001 only "hi", and so on.
 * Hadoop's built-in KeyFieldBasedPartitioner hashes the field instead, which can
 * put two languages in one reducer and leave another reducer empty.
 */
public class LangPartitioner implements Partitioner<Text, Text> {

    private String[] langs = {"en", "hi", "ta", "ja", "pt"};

    @Override
    public void configure(JobConf job) {
        String s = job.get("radar.langs");
        if (s != null && !s.trim().isEmpty()) {
            langs = s.trim().split(",");
        }
    }

    @Override
    public int getPartition(Text key, Text value, int numPartitions) {
        String k = key.toString();
        int tab = k.indexOf('\t');
        String lang = tab < 0 ? k : k.substring(0, tab);
        for (int i = 0; i < langs.length; i++) {
            if (langs[i].equals(lang)) {
                return i % numPartitions;
            }
        }
        return (lang.hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
