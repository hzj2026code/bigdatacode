package com.keshe;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;


public class Step2 {
    public static class CosineMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\t");
            if (line.length < 2) return;
            String[] items = line[1].split(",");
            for (int i = 0; i < items.length; i++) {
                String[] parts1 = items[i].split(":");
                String item1 = parts1[0];
                double rating1 = Double.parseDouble(parts1[1]);
                for (int j = 0; j < items.length; j++) {
                    String[] parts2 = items[j].split(":");
                    String item2 = parts2[0];
                    double rating2 = Double.parseDouble(parts2[1]);
                    double dotProduct = rating1 * rating2;
                    context.write(new Text(item1 + ":" + item2), new DoubleWritable(dotProduct));
                }
            }
        }
    }

    public static class CosineReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private Map<String, Double> normMap = new HashMap<>();
        @Override
        protected void setup(Context context) throws IOException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                Path path = new Path(cacheFiles[0]);
                FileSystem fs = FileSystem.get(context.getConfiguration());
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        String[] tokens = line.split("\t");
                        normMap.put(tokens[0], Double.parseDouble(tokens[1]));
                    }
                }
            }
        }

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double dotProductSum = 0.0;
            for (DoubleWritable val : values) {
                dotProductSum += val.get();
            }
            String[] items = key.toString().split(":");
            String item1 = items[0];
            String item2 = items[1];
            Double norm1 = normMap.get(item1);
            Double norm2 = normMap.get(item2);
            if (norm1 != null && norm2 != null && norm1 > 0 && norm2 > 0) {
                double cosineSim = dotProductSum / (norm1 * norm2);
                context.write(key, new DoubleWritable(cosineSim));
            }
        }
    }
}
