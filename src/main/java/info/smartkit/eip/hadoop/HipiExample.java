package info.smartkit.eip.hadoop;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by yangboz on 11/1/15.
 */
public class HipiExample extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        System.out.println("Hello HIPI!");
        return 0;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new HipiExample(), args);
        System.exit(0);
    }
}
