import java.io.FileNotFoundException;
import java.io.IOException;

import com.mysql.jdbc.PreparedStatement;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Connection;
import java.util.*;


/**
 * 读取Hdfs指定目录下文件大小和文件数量,并将结果写入mysql
 */
public class HdfsPathMonitor {
    // submit shell
    /*
     * main类的路径不需要指定，否则会被认为是参数传递进入。
     * yarn jar /app/m_user1/service/Hangzhou_HdfsFileMananger.jar /hive_tenant_account/hivedbname/
     */
    public static void main(String[] args) throws Exception {
        System.out.println("the args is " + String.join(",", args));
        String dirPath = args[0];

        Configuration conf = new Configuration();
        /*
         * <property> <name>fs.defaultFS</name> <value>hdfs://mycluster</value>
         * </property>
         */
        conf.set("fs.defaultFS", "hdfs://ns1");

        FileSystem fileSystem = FileSystem.get(conf);
        Path path = new Path(dirPath);

        // 获取文件列表
        FileStatus[] files = fileSystem.listStatus(path);
        if (files == null || files.length == 0) {
            throw new FileNotFoundException("Cannot access " + dirPath + ": No such file or directory.");
        }

        System.out.println("dirpath \t total file size \t total file count");

        for (int i = 0; i < files.length; i++) {
            System.out.println("files"+files);
            String pathStr = files[i].getPath().toString();

            String[] dir = pathStr.split("/");
            pathStr=dir[dir.length-1];
//            for (int j =0;j<dir.length;j++){
//                if (j==dir.length-1){
//                    pathStr=dir[i];
//                }
//            }

            FileSystem fs = files[i].getPath().getFileSystem(conf);
            long totalSize = fs.getContentSummary(files[i].getPath()).getLength();
            long totalFileCount = listAll(conf, files[i].getPath());
            fs.close();


            System.out.println(("".equals(pathStr) ? "." : pathStr) + "\t" + totalSize + "\t" + totalFileCount);


            SinkMysql(pathStr,totalSize,totalFileCount);
        }
    }

    /**
     * @Title: listAll @Description: 列出目录下所有文件 @return void 返回类型 @throws
     */
    public static Long listAll(Configuration conf, Path path) throws IOException {
        long totalFileCount = 0;
        FileSystem fs = FileSystem.get(conf);

        if (fs.exists(path)) {
            FileStatus[] stats = fs.listStatus(path);
            //System.out.println("-----------------------"+stats.length);
            for (int i = 0; i < stats.length; ++i) {
                //System.out.println("stats----------"+stats[i]);
                if (!stats[i].isDir()) {
                    // regular file
                    // System.out.println(stats[i].getPath().toString());
                    totalFileCount++;
                } else {
                    // dir
                    // System.out.println(stats[i].getPath().toString());
                    totalFileCount += listAll(conf, stats[i].getPath());
                }
            }
        }
        fs.close();

        return totalFileCount;
    }

    public static void SinkMysql(String pathStr,long totalSize,long totalFileCount) throws SQLException, ClassNotFoundException {
        try {
            // 加载数据库驱动
            Class.forName("com.mysql.jdbc.Driver");
            // 声明数据库view的URL
            String url = "jdbc:mysql://192.168.1.101:3306/vhr?useUnicode=true&characterEncoding=utf-8&useSSL=false";
            // 数据库用户名
            String user = "root";
            // 数据库密码
            String password = "hadoop";
            // 建立数据库连接，获得连接对象conn
            Connection conn = DriverManager.getConnection(url, user, password);
            String sql = "insert into test (totalsize,totalfiles,filedir) values(?,?,?)"; // 生成一条sql语句
            // 创建一个Statment对象
            PreparedStatement ps = (PreparedStatement) conn.prepareStatement(sql);

            // 为sql语句中第一个问号赋值

            ps.setLong(1, totalSize);
            ps.setLong(2, totalFileCount);
            ps.setString(3,pathStr);

            // 执行sql语句
            ps.executeUpdate();
            // 关闭数据库连接对象
            conn.close();
            System.out.println("jjk插入完毕！！！");
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}