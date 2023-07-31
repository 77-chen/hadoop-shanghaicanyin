package com.tjetc.mr.cy;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;



//2.输出数据到数据库
public class CYReducer extends Reducer<Text,CY, CY, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<CY> values, Context context)
            throws IOException, InterruptedException {
        // 定义变量用于存储数据
        double kwt = 0;
        double hjt = 0;
        double xjbt = 0;
        // 遍历values，计算
        for (CY tips : values) {
            kwt += tips.getKw();
            hjt += tips.getHj();
            xjbt += tips.getXjb();
        }

        //由于结果输出到数据库，所以不再输出到文件，此处new一个新对象，实现写入文件为空的操作
        CY tips = new CY();
        String qy = key.toString();
        tips.setQy(new Text(qy));
        tips.setKw(kwt);
        tips.setHj(hjt);
        tips.setXjb(xjbt);
        context.write(tips, NullWritable.get());
    }
}
