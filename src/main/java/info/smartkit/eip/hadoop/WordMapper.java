package info.smartkit.eip.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by yangboz on 10/28/15.
 */
public class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer lineTokenizer = new StringTokenizer(line);
        while (lineTokenizer.hasMoreTokens()) {
            String cleaned = removeNonLettersOrNumbers(lineTokenizer.nextToken());
            word.set(cleaned);
            context.write(word, new IntWritable(1));
        }
    }

    /**
     * Replaces all Unicode characters that are not either letters or numbers with
     * an empty string.
     *
     * @param original The original string.
     * @return A string that contains only letters and numbers.
     */
    private String removeNonLettersOrNumbers(String original) {
        return original.replaceAll("[^\\p{L}\\p{N}]", "");
    }
}
