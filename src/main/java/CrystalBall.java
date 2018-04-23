import java.io.IOException;
import java.util.HashMap;

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

        private java.util.Map<StringPair, Integer> map = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            this.map = new HashMap<StringPair, Integer>();
        }

        public void map(LongWritable key, Text value, Context context) {
            String line = value.toString();
            String[] result = line.split("\\s+");
            for (int i = 0; i < result.length; i++) {
                for (int j = i+1; j < result.length; j++) {
                    if (result[j].equals(result[i])) {
                        break;
                    }
                    StringPair pair = new StringPair(result[i], result[j]);
                    int count = 1;
                    if (this.map.containsKey(pair)) {
                        count += this.map.get(pair);
                    }
                    this.map.put(pair, count);
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            for (java.util.Map.Entry<StringPair, Integer> item: this.map.entrySet()) {
                context.write(item.getKey(), new IntWritable(item.getValue()));
            }
        }
    }
    public static class Reduce extends Reducer<StringPair, IntWritable, StringPair, FloatWritable> {

        private MyMapWritable G = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            this.G = new MyMapWritable();
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            for (java.util.Map.Entry<Writable, Writable> item: this.G.entrySet()) {
                Text key  = (Text)item.getKey();
                MyMapWritable H = (MyMapWritable)item.getValue();
                int total = H.getTotal();

                for (java.util.Map.Entry<Writable, Writable> item1: H.entrySet()) {
                    Writable neighbor = item1.getKey();
                    Writable neighborCount = item1.getValue();
                    float count = ((IntWritable)neighborCount).get();
                    context.write(new StringPair(key.toString(), neighbor.toString()), new FloatWritable(count / total));
                }
            }

        }

        public void reduce(StringPair pair, Iterable<IntWritable> values, Context context) {
            MyMapWritable newH = new MyMapWritable();
            for (IntWritable val : values) {
                int count = val.get();
                Text neighbor = new Text(pair.getSecond());
                if (newH.containsKey(neighbor)) {
                    count += ((IntWritable)newH.get(neighbor)).get();
                }
                newH.put(neighbor, new IntWritable(count));
            }

            Text key = new Text(pair.getFirst());
            if (this.G.containsKey(key)) {
                MyMapWritable originH = (MyMapWritable)this.G.get(key);
                newH.addAll(originH);
            }
            this.G.put(key, newH);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = new Job(conf, "crystal-ball-hadoop");
        job.setJarByClass(CrystalBall.class);

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