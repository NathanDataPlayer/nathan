import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;


public class DistributedCacheMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    HashMap<String, String> pdMap = new HashMap<>();
    @Override
    protected void setup(Mapper<LongWritable, Text, Text, NullWritable>.Context context)
            throws IOException, InterruptedException {

        URI[] cacheFiles = context.getCacheFiles();
        String path = cacheFiles[0].getPath().toString();
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path), "UTF-8"));
        String line;
        while(StringUtils.isNotEmpty(line = reader.readLine())){
            String[] fileds = line.split("\t");
            pdMap.put(fileds[0], fileds[1]);
        }
        IOUtils.closeStream(reader);
    }
    Text k = new Text();
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] fileds = line.split("\t");
        String pid = fileds[1];
        String pname = pdMap.get(pid);
        line = line +"\t"+ pname;
        k.set(line);
        context.write(k, NullWritable.get());
    }
}
