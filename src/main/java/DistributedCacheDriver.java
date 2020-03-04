import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DistributedCacheDriver {

    public static void main(String[] args) throws Exception, IOException {
        args = new String[] {"C:/Users/Administrator/Desktop/input","C:/Users/Administrator/Desktop/output"};

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(DistributedCacheDriver.class);
        job.setMapperClass(DistributedCacheMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.addCacheFile(new URI("file:///C:/Users/Administrator/Desktop/pdcache/pd.txt"));

        job.setNumReduceTasks(0);

        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
