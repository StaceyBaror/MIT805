import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class DelayPerYearCauseCount {
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] row = value.toString().split(",");

            String numTest = "\\d+";

            String arrDelay = row[2];
            boolean delayed = false;
            if (arrDelay.matches(numTest)) {
                delayed = Integer.parseInt(arrDelay) > 0;
            }

            String year = row[28];
            if (delayed) {
                if (row[27].matches(numTest)) {
                    boolean weather = Integer.parseInt(row[27]) > 0;
                    if (weather) {
                        String text = year + "-weather";
                        context.write(new Text(text), one);
                    }
                }

                if (row[9].matches(numTest)) {
                    boolean carrier = Integer.parseInt(row[9]) > 0;
                    if (carrier) {
                        String text = year + "-carrier";
                        context.write(new Text(text), one);
                    }
                }

                if (row[22].matches(numTest)) {
                    boolean security = Integer.parseInt(row[22]) > 0;
                    if (security) {
                        String text = year + "-security";
                        context.write(new Text(text), one);
                    }
                }

                if (row[18].matches(numTest)) {
                    boolean lateAircraft = Integer.parseInt(row[18]) > 0;
                    if (lateAircraft) {
                        String text = year + "-lateAircraft";
                        context.write(new Text(text), one);
                    }
                }

                if (row[20].matches(numTest)) {
                    boolean nas = Integer.parseInt(row[20]) > 0;
                    if (nas) {
                        String text = year + "-nas";
                        context.write(new Text(text), one);
                    }
                }
            }
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", ",");

        Job job = Job.getInstance(conf, "delay-cause-count");
        job.setJarByClass(DelayPerYearCauseCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
