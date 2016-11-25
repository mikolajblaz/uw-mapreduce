import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URI;
import java.util.Arrays;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * Created by mikib on 23.11.16.
 */

public class TeraSort extends Configured implements Tool {

    private final static int numRange = 1000;
    private final static String samplesPath = "samples_output";

    private static float threshold;
    private static Random r = new Random();

    // Sampler
    public static class SampleMapper extends Mapper<Text, Text, IntWritable, IntWritable>{
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            IntWritable k = new IntWritable(Integer.parseInt(key.toString()));
            IntWritable v = new IntWritable(Integer.parseInt(value.toString()));

            // Emit only sample
            if (r.nextFloat() < threshold)
                context.write(k, v);
        }
    }

    public static class SampleReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class SamplePartitioner extends Partitioner<IntWritable, IntWritable> {
        @Override
        public int getPartition(IntWritable key, IntWritable value, int numberOfPartitions) {
            // send whole sample to one reducer
            return 0;
        }
    }

    // Sorter
    public static class SortMapper extends Mapper<Text, Text, IntWritable, IntWritable>{

        protected int[] borders;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            URI samplesURI = context.getCacheFiles()[0];
            Path samplesPath = new Path(samplesURI.getPath());
            countBorders(samplesPath);
        }

        protected void countBorders(Path samplesPath) {
            borders = new int[10]; // TODO
        }

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            int intKey = Integer.parseInt(key.toString());
            int intValue = Integer.parseInt(value.toString());
            IntWritable k = new IntWritable(intKey);
            IntWritable v = new IntWritable(intValue);

            context.write(k, v);
        }
    }

    public static class SortReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
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

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        // Job
        Job sampleJob = Job.getInstance(conf, "TeraSort sampling");
        sampleJob.setJarByClass(TeraSort.class);
        // Map-Combine-Reduce
        sampleJob.setMapperClass(SampleMapper.class);
        sampleJob.setReducerClass(SampleReducer.class);
        sampleJob.setNumReduceTasks(1); // only one reducer here!
        sampleJob.setPartitionerClass(SamplePartitioner.class);
        // Input
        sampleJob.setInputFormatClass(KeyValueTextInputFormat.class);
        KeyValueTextInputFormat.addInputPath(sampleJob, new Path(args[0]));
        // Output
        sampleJob.setOutputKeyClass(IntWritable.class);
        sampleJob.setOutputValueClass(IntWritable.class);
        FileOutputFormat.setOutputPath(sampleJob, new Path(samplesPath));

        boolean status = sampleJob.waitForCompletion(true);
        if (!status)
            return 1;

        // Job
        Job sortJob = Job.getInstance(conf, "TeraSort sorting");
        sortJob.setJarByClass(TeraSort.class);
        // Map-Combine-Reduce
        sortJob.setMapperClass(SortMapper.class);
        sortJob.setReducerClass(SortReducer.class);
        sortJob.setPartitionerClass(SortPartitioner.class);
        // Input
        sortJob.setInputFormatClass(KeyValueTextInputFormat.class);
        KeyValueTextInputFormat.addInputPath(sortJob, new Path(args[0]));
        sortJob.addCacheFile(new URI(samplesPath)); // TODO: add hdfs:// to uri
        // Output
        sortJob.setOutputKeyClass(IntWritable.class);
        sortJob.setOutputValueClass(IntWritable.class);
        FileOutputFormat.setOutputPath(sortJob, new Path(args[1]));
        return sortJob.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();

        if (remainingArgs.length != 3) {
            System.err.println("Usage: TeraSort <InputDirectory> <OutputDirectory> <Threshold>");
            System.exit(0);
        }
        threshold = Float.parseFloat(remainingArgs[2]);

        int ret = ToolRunner.run(optionParser.getConfiguration(), new TeraSort(), remainingArgs);
        System.exit(ret);
    }
}
