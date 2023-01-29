package kmeans;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.ArrayList;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;


public class KMeans3 extends Configured implements Tool{
    // Use maximum iterations = 20 as required
    private final static int maxIterations = 20;
    // Modify K value to 3 or 6 when needed
    private final static int K = 3;

    public static class Point implements Comparable<Point> {
        private final double x;
        private final double y;

        public Point(String s) {
            // Split x, y from point data by ','
            String[] strings = s.split(",");
            this.x = Double.parseDouble(strings[0]);
            this.y = Double.parseDouble(strings[1]);
        }

        public Point(double x, double y) {
            this.x = x;
            this.y = y;
        }

        public double getX() {
            return this.x;
        }

        public double getY() {
            return this.y;
        }

        public static double calculateDistance(Point point1, Point point2) {
            double x_diff = point1.getX() - point2.getX();
            double y_diff = point1.getY() - point2.getY();
            return Math.sqrt(Math.pow(x_diff,2) + Math.pow(y_diff,2));
        }

        @Override
        public int compareTo(Point o) {
            int compareX = Double.compare(this.getX(), o.getX());
            int compareY = Double.compare(this.getY(), o.getY());

            return compareX != 0 ? compareX : compareY;
        }

        public String toString() {
            return this.x + "," + this.y;
        }

        public static void writePointsToFile(List<Point> points, Configuration conf) throws IOException{
            Path centroid_path = new Path(conf.get("centroid.path"));
            FileSystem fs = FileSystem.get(conf);
            if (fs.exists(centroid_path)) {
                fs.delete(centroid_path, true);
            }

            final SequenceFile.Writer centerWriter = SequenceFile.createWriter(fs, conf, centroid_path, Text.class,
                    IntWritable.class);
            final IntWritable value = new IntWritable(0);

            for (Point point : points) {
                centerWriter.append(new Text(point.toString()), value);
            }

            centerWriter.close();
        }

    }

    public static class PointsMapper extends Mapper<LongWritable, Text, Text, Text> {

        public List<Point> centers = new ArrayList<>();

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            ArrayList<Point> arrayList = new ArrayList<>();
            Configuration conf = context.getConfiguration();
            Path center_path = new Path(conf.get("centroid.path"));
            FileSystem fs = FileSystem.get(conf);
            SequenceFile.Reader reader = new SequenceFile.Reader(fs, center_path, conf);
            Text key = new Text();
            IntWritable value = new IntWritable();
            while (reader.next(key, value)) {
                arrayList.add(new Point(key.toString()));
            }
            reader.close();
            this.centers = arrayList;
        }

        // Map
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // input -> key: charater offset, value -> a point (in Text)
            Point point = new Point(value.toString());
            // Write logic to assign a point to a centroid
            int index = -1;
            double min = Double.MAX_VALUE;
            for (int i = 0; i < centers.size(); i++) {
                double temp = Point.calculateDistance(point, centers.get(i));
                if (temp < min) {
                    min = temp;
                    index = i;
                }
            }
            // Emit key (centroid id/centroid) and value (point)
            context.write(new Text(Integer.toString(index)), new Text(point.toString()));
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
        }

    }

    public static class PointsReducer extends Reducer<Text, Text, Text, Text> {

        public List<Point> new_centers = new ArrayList<>();

        public enum Counter {
            CONVERGED
        }

        @Override
        public void setup(Context context) {}

        // Reduce
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Input: key -> centroid id/centroid , value -> list of points
            // Calculate the new centroid
            double sum_of_X = 0;
            double sum_of_Y = 0;
            int count = 0;
            while (values.iterator().hasNext()) {
                count++;
                String line = values.iterator().next().toString();
                Point point = new Point(line);
                sum_of_X = sum_of_X + point.getX();
                sum_of_Y = sum_of_Y + point.getY();
            }

            // new_centroids.add() (store updated cetroid in a variable)
            double new_center_X = sum_of_X / count;
            double new_center_Y = sum_of_Y / count;
            Point center = new Point(new_center_X + "," + new_center_Y);
            new_centers.add(center);

            context.write(key, new Text(center.toString()));
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            // BufferedWriter
            // Delete the old centroids and write the new centroids
            super.setup(context);
            Configuration conf = context.getConfiguration();
            Point.writePointsToFile(this.new_centers, conf);
        }
    }


    private static List<Point> initiateCenterPoints() {
        List<Point> list = new ArrayList<>();
        Random random = new Random();
        int min = -50;
        int max = 50;

        HashSet<Integer> set1 = new HashSet<>();
        HashSet<Integer> set2 = new HashSet<>();
        for(int i = 1; i <= KMeans3.K; i++){
            int rand1 = random.nextInt(max-min)+min;
            int rand2 = random.nextInt(max-min)+min;
            while(set1.contains(rand1) || set2.contains(rand2) ){
                rand1 = random.nextInt(max-min)+min;
                rand2 = random.nextInt(max-min)+min;
            }
            set1.add(rand1);
            set2.add(rand2);
            list.add(new Point(rand1, rand2));
        }
        return list;

    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Path center_path = new Path("centroid/cen.seq");
        conf.set("centroid.path", center_path.toString());

        Point.writePointsToFile(initiateCenterPoints(), conf);

        long stime = System.currentTimeMillis();

        int i = 1;
        // Use maximum iterations = 20
        while(i <= maxIterations){
            ToolRunner.run(conf, new KMeans3(), args);
            i++;
        }
        long etime = System.currentTimeMillis();

        long duration = (etime - stime)/1000;

        System.out.println("The total running time is " + duration);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "KMeans");
        FileSystem fs=FileSystem.get(conf);

        job.setJarByClass(KMeans3.class);
        job.setMapperClass(PointsMapper.class);
        job.setReducerClass(PointsReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path output = new Path (args[1]);
        fs.delete(output,true);
        FileOutputFormat.setOutputPath(job, output);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
