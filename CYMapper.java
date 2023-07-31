package com.tjetc.mr.cy;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;



//2.输出数据到数据库
// 定义Mapper的输入键类型为LongWritable，输入值类型为Text，输出键类型为Text，输出值类型为CY
public class CYMapper extends Mapper<LongWritable, Text, Text, CY> {

    //声明私有成员变量date和tipData，分别用于存储行政区和餐饮数据。
    private Text qy = new Text();
    private CY cyData = new CY();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // 将输入的Text类型转换为String类型
        String line = value.toString();
        String[] data = line.split("\t");
        String strkw;
        String strhj;

        strkw = data[4];
        strhj = data[5];
        //行政区数据
        String xzq = data[2];

        //CY.setKw(Double.parseDouble(strkw));

        // 设置输出键和值qy
        qy.set(xzq);
        cyData.setQy(qy);
        cyData.setKw(Double.parseDouble(strkw));
        cyData.setHj(Double.parseDouble(strhj));
        cyData.setXjb((Double.parseDouble(strkw)+Double.parseDouble(strhj))/2);

        context.write(qy, cyData);

        }

}
