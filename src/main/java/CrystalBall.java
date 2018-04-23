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

    public static class Map extends Mapper<LongWritable, Text, StringPair, FloatWritable> {
        public static final Log log = LogFactory.getLog(Map.class);
        private final static FloatWritable one = new FloatWritable(1);

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            log.info("line: " + line);
            String[] result = line.split("\\s+");
            log.info("result: " + result);
            for (String item: result) {
                log.info(item);
            }
            log.info("result.length: " + result.length);
            for (int i = 0; i < result.length; i++) {
                log.info("i = " + i);
                for (int j = i+1; j < result.length; j++) {
                    log.info("j = " + j);
                    if (result[j].equals(result[i])) {
                        log.info("break");
                        break;
                    }
                    log.info(new StringPair(result[i], result[j]) + ", 1");
                    context.write(new StringPair(result[i], "*"), one);
                    context.write(new StringPair(result[i], result[j]), one);
                }
            }
        }

    }
    public static class Reduce extends Reducer<StringPair, FloatWritable, StringPair, FloatWritable> {
        public static final Log log = LogFactory.getLog(Map.class);
        private float total = 0;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            this.total = 0;
        }

        public void reduce(StringPair key, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException {
            float sum = 0;
            for (FloatWritable val : values) {
                sum += val.get();
            }
            if (key.getSecond().equals("*")) {
                total = sum;
            } else {
                log.info("sum=" + sum);
                log.info("total=" + total);
                log.info("sum/total = " + sum/total);
                context.write(key, new FloatWritable(sum/total));
            }

        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = new Job(conf, "crystal-ball-hadoop");
        job.setJarByClass(CrystalBall.class);

        job.setMapOutputKeyClass(StringPair.class);
        job.setMapOutputValueClass(FloatWritable.class);
        job.setOutputKeyClass(StringPair.class);
        job.setOutputValueClass(FloatWritable.class);


        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}