import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * Created by mikib on 23.11.16.
 */

public class TeraSort {

    final static int numRange = 1000;

    public static class IntMapper
            extends Mapper<Text, Text, IntWritable, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private final static IntWritable two = new IntWritable(2);
        private final static Random r = new Random();

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            int intKey = Integer.parseInt(key.toString());
            int intValue = Integer.parseInt(value.toString());
            IntWritable k = new IntWritable(intKey);
            IntWritable v = new IntWritable(intValue);

            context.write(k, v);
        }
    }

    public static class IntSumReducer
            extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class SortPartitioner extends Partitioner<IntWritable, IntWritable> {
        @Override
        public int getPartition(IntWritable key, IntWritable value, int numberOfPartitions) {
            return key.get() / (numRange / numberOfPartitions);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");
        // Job
        Job job = Job.getInstance(conf, "TeraSort");
        job.setJarByClass(TeraSort.class);
        // Map-Combine-Reduce
        job.setMapperClass(IntMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setNumReduceTasks(3);
        job.setPartitionerClass(SortPartitioner.class);
        // Input
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        KeyValueTextInputFormat.addInputPath(job, new Path(args[0]));
        // Output
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean status = job.waitForCompletion(true);
        System.exit(status ? 0 : 1);
    }
}
