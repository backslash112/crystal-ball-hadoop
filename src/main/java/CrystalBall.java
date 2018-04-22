import java.io.IOException;

import com.sun.corba.se.spi.ior.Writeable;
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

    public static class Map extends Mapper<LongWritable, Text, Text, MyMapWritable> {

        private final static IntWritable one = new IntWritable(1);

        public void map(LongWritable mk, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] result = line.split("\\s+");

            for (int i = 0; i < result.length; i++) {
                String key = result[i];
                MyMapWritable H = new MyMapWritable();
                for (int j = i+1; j < result.length; j++) {
                    if (result[j].equals(key)) {
                        break;
                    }
                    String neighborStr = result[j];
                    Text neighbor = new Text(neighborStr);
                    if (H.containsKey(neighbor)) {
                        H.put(neighbor, new IntWritable(((IntWritable)H.get(neighbor)).get() + 1));
                    } else {
                        H.put(neighbor, one);
                    }
                }

                if (!H.isEmpty()) {
                    context.write(new Text(key), H);
                }
            }
        }
    }
    public static class Reduce extends Reducer<Text, MyMapWritable, Text, Writable> {

        public void reduce(Text key, Iterable<MyMapWritable> values, Context context)
                throws IOException, InterruptedException {
            MyMapWritable H = new MyMapWritable();
            for (MyMapWritable val : values) {
                for (java.util.Map.Entry<Writable, Writable> item: val.entrySet()) {
                    Writable itemKey = item.getKey();
                    if (H.containsKey(itemKey)) {
                        Writable itemValue = item.getValue();
                        Writable originValue = H.get(itemKey);
                        int newValue = ((IntWritable)itemValue).get() + ((IntWritable)originValue).get();
                        H.put(itemKey, new IntWritable(newValue));
                    } else {
                        H.put(itemKey, item.getValue());
                    }
                }
            }
            context.write(key, H);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = new Job(conf, "crystal-ball-hadoop");
        job.setJarByClass(CrystalBall.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MyMapWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Writable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}