package com.keshe;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Step3 {
    public static class RecMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Map<String, Map<String, Double>> matrix = new HashMap<>();
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
                        if (tokens.length == 2) {
                            String[] items = tokens[0].split(":");
                            String item1 = items[0];
                            String item2 = items[1];
                            double score = Double.parseDouble(tokens[1]);
                            matrix.computeIfAbsent(item1, k -> new HashMap<>()).put(item2, score);
                        }
                    }
                }
            }
        }
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\t");
            if (tokens.length < 2) return;
            String userId = tokens[0];
            String[] couples = tokens[1].split(",");
            Set<String> set = new HashSet<>();
            Map<String, Double> rates = new HashMap<>();
            for (String cp : couples) {
                String[] parts = cp.split(":");
                if (parts.length == 2) {
                    try {
                        set.add(parts[0]);
                        rates.put(parts[0], Double.parseDouble(parts[1]));
                    } catch (NumberFormatException ignored) {}
                }
            }
            Map<String, Double> recommend = new HashMap<>();
            for (Map.Entry<String, Double> entry : rates.entrySet()) {
                String i = entry.getKey();
                double rating = entry.getValue();
                if (matrix.containsKey(i)) {
                    for (Map.Entry<String, Double> simEntry : matrix.get(i).entrySet()) {
                        String similari = simEntry.getKey();
                        double similarity = simEntry.getValue();
                        if (!set.contains(similari)) {
                            double score = rating * similarity;
                            recommend.merge(similari, score, Double::sum);
                        }
                    }
                }
            }
            for (Map.Entry<String, Double> entry : recommend.entrySet()) {
                context.write(new Text(userId), new Text(entry.getKey() + ":" + entry.getValue()));
            }
        }
    }
    public static class RecReducer extends Reducer<Text, Text, Text, Text> {
        private static class recommend implements Comparable<recommend> {
            String id;
            double score;
            public recommend(String id, double score) {
                this.id = id;
                this.score = score;
            }
            @Override
            public int compareTo(recommend other) {
                return Double.compare(other.score, this.score);
            }
        }
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Double> recommends = new HashMap<>();
            for (Text value : values) {
                String[] parts = value.toString().split(":");
                if (parts.length == 2) {
                    String id = parts[0];
                    double score = Double.parseDouble(parts[1]);
                    recommends.merge(id, score, Double::sum);
                }
            }
            List<recommend> sortedrecommends = new ArrayList<>();
            for (Map.Entry<String, Double> entry : recommends.entrySet()) {
                sortedrecommends.add(new recommend(entry.getKey(), entry.getValue()));
            }
            Collections.sort(sortedrecommends);
            StringBuilder result = new StringBuilder();
            int count = 0;
            for (recommend rec : sortedrecommends) {
                if (count >= 10) break;
                result.append(rec.id).append(":").append(String.format("%.2f", rec.score)).append(",");
                count++;
            }
            if (result.length() > 0) {
                result.setLength(result.length() - 1);
                context.write(key, new Text(result.toString()));
            }
        }
    }
}
