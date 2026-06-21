package com.keshe;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class Nearby {
    private static final double EARTH_RADIUS = 6371.0;
    private static double haversine(double lat1, double lon1, double lat2, double lon2) {
        double lat = Math.toRadians(lat2 - lat1);
        double lon = Math.toRadians(lon2 - lon1);
        double a = Math.sin(lat / 2) * Math.sin(lat / 2) 
                 + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) 
                 * Math.sin(lon / 2) * Math.sin(lon / 2);
        return EARTH_RADIUS * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    }

    public static class TopNMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
        private TreeMap<Double, String> localTop6 = new TreeMap<>();
        private double targetLat, targetLon;

        @Override
        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            targetLat = Double.parseDouble(conf.get("target.lat", "0.0"));
            targetLon = Double.parseDouble(conf.get("target.lon", "0.0"));
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) {
            String[] tokens = value.toString().split("\t");
            if (tokens.length >= 5) {
                String id = tokens[0], name = tokens[3], stars = tokens[4];
                double lat = Double.parseDouble(tokens[1]), lon = Double.parseDouble(tokens[2]);
                double distance = haversine(targetLat, targetLon, lat, lon);
                localTop6.put(distance + (Math.random() * 0.000001), 
                        id + " | " + name + " | " + stars + "星 | 距离: " + String.format("%.2f", distance) + " km");
                if (localTop6.size() > 6) localTop6.remove(localTop6.lastKey());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Double, String> entry : localTop6.entrySet()) 
                context.write(new DoubleWritable(entry.getKey()), new Text(entry.getValue()));
        }
    }

    public static class TopNReducer extends Reducer<DoubleWritable, Text, NullWritable, Text> {
        private TreeMap<Double, String> globalTop6 = new TreeMap<>();

        @Override
        protected void reduce(DoubleWritable key, Iterable<Text> values, Context context) {
            for (Text val : values) {
                globalTop6.put(key.get(), val.toString());
                if (globalTop6.size() > 6) globalTop6.remove(globalTop6.lastKey());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            int rank = 1;
            for (Map.Entry<Double, String> entry : globalTop6.entrySet()) {
                context.write(NullWritable.get(), new Text("附近推荐 TOP " + rank + " -> " + entry.getValue()));
                rank++;
            }
        }
    }
}
