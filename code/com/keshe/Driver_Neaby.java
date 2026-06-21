package com.keshe;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.BufferedReader;
import java.io.InputStreamReader;

public class Driver_Nearby {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("用法: Driver_Nearby <纬度> <经度>");
            System.exit(1);
        }
        Configuration conf = new Configuration();
        conf.set("target.lat", args[0]);
        conf.set("target.lon", args[1]);
        Job job = Job.getInstance(conf, "Nearby_Search_Temp");
        job.setJarByClass(Driver_Nearby.class);
        job.setMapperClass(Nearby.TopNMapper.class);
        job.setReducerClass(Nearby.TopNReducer.class);
        job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(org.apache.hadoop.io.DoubleWritable.class);
        job.setMapOutputValueClass(org.apache.hadoop.io.Text.class);
        job.setOutputKeyClass(org.apache.hadoop.io.NullWritable.class);
        job.setOutputValueClass(org.apache.hadoop.io.Text.class);
        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/keshe/businesses.tsv"));
        Path tempOutPath = new Path("hdfs://localhost:9000/keshe/tmp_nearby_" + System.currentTimeMillis());
        FileOutputFormat.setOutputPath(job, tempOutPath);
        boolean success = job.waitForCompletion(true);
        if (success) {
            System.out.println("\n============================= 附近餐馆智能推荐 =============================");
            FileSystem fs = FileSystem.get(conf);
            Path resFile = new Path(tempOutPath, "part-r-00000");
            if (fs.exists(resFile)) {
                try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(resFile)))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        System.out.println(line);
                    }
                }
            } else {
                System.out.println("抱歉，附近未能找到符合条件的餐馆。");
            } System.out.println("===========================================================================\n");
            fs.delete(tempOutPath, true);
        }
        System.exit(success ? 0 : 1);
    }
}
