import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class CrystalBall {

    public static class Map extends Mapper<LongWritable, Text, StringPair, IntWritable> {
        public static final Log log = LogFactory.getLog(Map.class);
        private final static IntWritable one = new IntWritable(1);

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] result = line.split("\\s+w");
            log.info(result);
            for (String item: result) {
                log.info(item);
            }
            for (int i = 0; i < result.length; i++) {
                for (int j = i+1; j < result.length; j++) {
                    if (result[j] == result[i]) {
                       break;
                    }
                    context.write(new StringPair(result[i], result[j]), one);
                }
            }
        }

    }
    public static class Reduce extends Reducer<StringPair, IntWritable, StringPair, IntWritable> {

        public void reduce(StringPair key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = new Job(conf, "crystal-ball-hadoop");
        job.setJarByClass(CrystalBall.class);
        job.setOutputKeyClass(StringPair.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}