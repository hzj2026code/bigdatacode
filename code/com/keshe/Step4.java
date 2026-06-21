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
import java.util.List;
import java.util.Map;

public class Step4 {
    private static final double EARTH_RADIUS = 6371.0;
    private static double haversine(double lat1, double lon1, double lat2, double lon2) {
        double lat = Math.toRadians(lat2 - lat1);
        double lon = Math.toRadians(lon2 - lon1);
        double a = Math.sin(lat / 2) * Math.sin(lat / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(lon / 2) * Math.sin(lon / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return EARTH_RADIUS * c;
    }
    public static class LocMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Map<String, String> locMap = new HashMap<>();
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
                        if (tokens.length == 3) {
                            locMap.put(tokens[0], tokens[1] + "," + tokens[2]);
                        }
                    }
                }
            }
        }
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\t");
            String userId = tokens[0];
            String[] items = tokens[1].split(",");       
            for (String item : items) {
                String storeId = item.split(":")[0];
                if (locMap.containsKey(storeId)) {
                    context.write(new Text(userId), new Text("LOC:" + locMap.get(storeId)));
                }
            }
        }
    }
    public static class RecMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\t");
            context.write(new Text(tokens[0]), new Text("REC:" + tokens[1]));
        }
    }
    public static class FinalReducer extends Reducer<Text, Text, Text, Text> {
        private Map<String, String> locMap = new HashMap<>();

        private static class Recommend implements Comparable<Recommend> {
            String storeId; double score;
            public Recommend(String storeId, double score) { this.storeId = storeId; this.score = score; }
            @Override public int compareTo(Recommend other) { return Double.compare(other.score, this.score); }
        }
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
                        if (tokens.length == 3) {
                            locMap.put(tokens[0], tokens[1] + "," + tokens[2]);
                        }
                    }
                }
            }
        }
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> userLocs = new ArrayList<>();
            String recsLine = null;
            for (Text val : values) {
                String line = val.toString();
                if (line.startsWith("LOC:")) {
                    userLocs.add(line.substring(4));
                } else if (line.startsWith("REC:")) {
                    recsLine = line.substring(4);
                }
            }
            if (userLocs.isEmpty() || recsLine == null) return;
            double sumLat = 0.0, sumLon = 0.0;
            for (String loc : userLocs) {
                String[] parts = loc.split(",");
                sumLat += Double.parseDouble(parts[0]);
                sumLon += Double.parseDouble(parts[1]);
            }
            double avgLat = sumLat / userLocs.size();
            double avgLon = sumLon / userLocs.size();
            List<Recommend> finalRecs = new ArrayList<>();
            String[] recs = recsLine.split(",");
            for (String rec : recs) {
                String[] parts = rec.split(":");
                String storeId = parts[0];
                double oscore = Double.parseDouble(parts[1]);
                if (locMap.containsKey(storeId)) {
                    String[] locParts = locMap.get(storeId).split(",");
                    double recLat = Double.parseDouble(locParts[0]);
                    double recLon = Double.parseDouble(locParts[1]);
                    double distance = haversine(avgLat, avgLon, recLat, recLon);
                    double finalScore = oscore / (1 + distance);        
                    finalRecs.add(new Recommend(storeId, finalScore));
                }
            }
            Collections.sort(finalRecs);
            StringBuilder result = new StringBuilder();
            int count = 0;
            for (Recommend rec : finalRecs) {
                if (count >= 10) break;
                String locInfo = locMap.get(rec.storeId);
                result.append(rec.storeId).append(":")
                    .append(String.format("%.2f", rec.score)).append(":")
                    .append(locInfo).append(",");
                count++;
            }
            if (result.length() > 0) {
                result.setLength(result.length() - 1);
                context.write(key, new Text(result.toString()));
            }
        }
    }
}
