import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;

public class HDFSUpload {
    public static void main(String[] args) throws IOException {
        //预定义环境变量
        System.setProperty("HADOOP_USER_NAME", "root");

        //第一步：加载配置，构造客户端
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://ns1");
        FileSystem client = FileSystem.get(conf);

        //第二步：构造输入流
        InputStream input = client.open(new Path("/test/a.txt"));

        //第三步：构造输出流
        // File file = new File("D:\\test_download.txt");
        OutputStream output = new FileOutputStream("D:\\test_download.txt");

        //第四步：通过IOUtils文件传输
        IOUtils.copyBytes(input, output, 1024);
        System.out.println("ok");

        //第五步：关闭流
        input.close();
        output.close();
        client.close();

    }
}