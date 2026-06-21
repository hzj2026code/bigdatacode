package com.keshe;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;


public class Step2A {

    public static class NormMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\t");
            if (tokens.length >= 3) {
                String businessId = tokens[1];
                double rating = Double.parseDouble(tokens[2]);
                // 输出: business_id, 评分的平方
                context.write(new Text(businessId), new DoubleWritable(rating * rating));
            }
        }
    }

    public static class NormReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sumOfSquares = 0.0;
            for (DoubleWritable val : values) {
                sumOfSquares += val.get();
            }
            // 计算平方根，得到 L2 范数
            double norm = Math.sqrt(sumOfSquares);
            context.write(key, new DoubleWritable(norm));
        }
    }
}
