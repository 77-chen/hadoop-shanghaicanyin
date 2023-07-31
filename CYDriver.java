package com.tjetc.mr.cy;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapred.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;


import java.io.IOException;


public class CYDriver {
    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException {


        //设置运行的用户名
        System.setProperty("HADOOP_USER_NAME", "icss");
        //获取job对象
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.137.110:9000");
        //jdbc:mysql://192.168.137.1:3306/tips    tips表示数据名称
        DBConfiguration.configureDB(conf, "com.mysql.cj.jdbc.Driver",
                "jdbc:mysql://192.168.137.1:3306/yd", "root", "123456");
        Job job = Job.getInstance(conf, "shdata");
        //设置运行jar
        job.setJarByClass(com.tjetc.mr.cy.CYDriver.class);

        //设置自定的mapper和reducer
        job.setMapperClass(CYMapper.class);
        job.setReducerClass(CYReducer.class);
        //job.setPartitionerClass(CYPartitioner.class);

        // 设置输出Key和Value的类
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CY.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(CY.class);

        job.setInputFormatClass(TextInputFormat.class);
        // 设置输出格式为DBOutputFormat
        job.setOutputFormatClass(DBOutputFormat.class);
        job.setNumReduceTasks(1);

        //输入和输出路径
        FileInputFormat.setInputPaths(job, new Path("/input/shdata"));
        //tips是表名称 "date","min", "max", "sum", "count", "avg" 是字段名称
        DBOutputFormat.setOutput(job, "cy",  "qy","kw", "hj", "xjb");

        //提交job，等待运行结果,阻塞代码，等待运行结果
        boolean result = job.waitForCompletion(true);
        //程序退出（0表示正常退出，非0表示异常退出）
        System.exit(result ? 0 : 1);
    }
}
