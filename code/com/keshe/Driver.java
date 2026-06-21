package com.keshe;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.net.URI;

public class Driver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // Job 1
        Job job1 = Job.getInstance(conf, "Step1_UserList");
        job1.setJarByClass(Driver.class);
        job1.setMapperClass(Step1.UserMapper.class);
        job1.setReducerClass(Step1.UserReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path("hdfs://localhost:9000/keshe/reviews.tsv"));
        Path outPath1 = new Path("hdfs://localhost:9000/keshe/output_step1");
        outPath1.getFileSystem(conf).delete(outPath1, true);
        FileOutputFormat.setOutputPath(job1, outPath1);
        if (!job1.waitForCompletion(true)) { System.exit(1); }

        // Job 2A
        Job job2a = Job.getInstance(conf, "Step2A_ItemNorm");
        job2a.setJarByClass(Driver.class);
        job2a.setMapperClass(Step2A.NormMapper.class);
        job2a.setCombinerClass(Step2A.NormReducer.class);
        job2a.setReducerClass(Step2A.NormReducer.class);
        job2a.setOutputKeyClass(Text.class);
        job2a.setOutputValueClass(org.apache.hadoop.io.DoubleWritable.class);
        FileInputFormat.addInputPath(job2a, new Path("hdfs://localhost:9000/keshe/reviews.tsv"));
        Path outPath2a = new Path("hdfs://localhost:9000/keshe/output_step2a");
        outPath2a.getFileSystem(conf).delete(outPath2a, true);
        FileOutputFormat.setOutputPath(job2a, outPath2a);
        if (!job2a.waitForCompletion(true)) { System.exit(1); }
        Job job2 = Job.getInstance(conf, "Step2_CosineSim");
        job2.setJarByClass(Driver.class);
        job2.addCacheFile(new URI("hdfs://localhost:9000/keshe/output_step2a/part-r-00000"));
        job2.setMapperClass(Step2.CosineMapper.class);
        job2.setReducerClass(Step2.CosineReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(org.apache.hadoop.io.DoubleWritable.class);
        FileInputFormat.addInputPath(job2, outPath1);
        Path outPath2 = new Path("hdfs://localhost:9000/keshe/output_step2");
        outPath2.getFileSystem(conf).delete(outPath2, true);
        FileOutputFormat.setOutputPath(job2, outPath2);
        if (!job2.waitForCompletion(true)) { System.exit(1); }

        // Job 3
        Job job3 = Job.getInstance(conf, "Step3_Recommend");
        job3.setJarByClass(Driver.class);
        job3.addCacheFile(new URI("hdfs://localhost:9000/keshe/output_step2/part-r-00000"));
        job3.setMapperClass(Step3.RecMapper.class);
        job3.setReducerClass(Step3.RecReducer.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job3, outPath1);
        Path outPath3 = new Path("hdfs://localhost:9000/keshe/output_step3");
        outPath3.getFileSystem(conf).delete(outPath3, true);
        FileOutputFormat.setOutputPath(job3, outPath3);
        if (!job3.waitForCompletion(true)) { System.exit(1); }
        
        // Job 4
        Job job4 = Job.getInstance(conf, "Step4_LocationRec");
        job4.setJarByClass(Driver.class);
        job4.addCacheFile(new URI("hdfs://localhost:9000/keshe/businesses.tsv"));
        MultipleInputs.addInputPath(job4, outPath1, TextInputFormat.class, Step4.LocMapper.class);
        MultipleInputs.addInputPath(job4, outPath3, TextInputFormat.class, Step4.RecMapper.class);
        job4.setReducerClass(Step4.FinalReducer.class);
        job4.setMapOutputKeyClass(Text.class);
        job4.setMapOutputValueClass(Text.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);
        Path outPath4 = new Path("hdfs://localhost:9000/keshe/output_final");
        outPath4.getFileSystem(conf).delete(outPath4, true);
        FileOutputFormat.setOutputPath(job4, outPath4);
        System.exit(job4.waitForCompletion(true) ? 0 : 1);
    }
}
