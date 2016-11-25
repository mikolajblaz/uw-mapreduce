import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
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
    private final static String samplesPath = "samples_output/";
    private final static String samplesFile = "part-r-00000";

    private static float threshold;
    private static Random r = new Random();

    // Sampler
    public static class SampleMapper extends Mapper<Text, Text, IntWritable, IntWritable>{
        private final static IntWritable zero = new IntWritable(0);
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            IntWritable k = new IntWritable(Integer.parseInt(key.toString()));

            // Emit only sample to one reducer
            if (r.nextFloat() < threshold)
                context.write(zero, k);
        }
    }

    public static class SampleReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        private IntWritable result = new IntWritable();
        @Override
        public void reduce(IntWritable zero, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            List<Integer> keys = new LinkedList<>();
            for (IntWritable key : values)
                keys.add(key.get());
            Integer[] intKeys = keys.toArray(new Integer[keys.size()]);

            int reducersNum = Integer.parseInt(context.getConfiguration().get("my.reducers"));
            Integer[] borders = chooseBorders(intKeys, reducersNum);

            for (Integer key : borders) {
                result.set(key);
                context.write(result, result);
            }
        }

        private Integer[] chooseBorders(Integer[] keys, int reducersNum) {
            Arrays.sort(keys);
            int s = keys.length;
            Integer[] borders = new Integer[reducersNum - 1];
            for (int i = 0; i < reducersNum - 1; i++) {
                borders[i] = keys[(i + 1) * s / reducersNum];
            }
            return borders;
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
            countBorders(samplesPath, conf);
        }

        protected void countBorders(Path samplesPath, Configuration conf) throws IOException {
            BufferedReader br;
            String line;
            FSDataInputStream input = null;
            FileSystem fs = FileSystem.get(conf);

            int reducersNum = Integer.parseInt(conf.get("my.reducers"));
            borders = new int[reducersNum + 1];
            // set guards
            borders[0] = Integer.MIN_VALUE;
            borders[reducersNum] = Integer.MAX_VALUE;

            try {
                input = fs.open(samplesPath);
                br = new BufferedReader(new InputStreamReader(input));
                int i = 1;

                while ((line = br.readLine()) != null && !line.equals("")) {
                    borders[i] = Integer.parseInt(line.split("\\t")[0]);
                    i++;
                }

            } finally {
                IOUtils.closeStream(input);
            }
            System.out.println("################ BORDERS ########################");
            System.out.println(Arrays.toString(borders));
        }

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            IntWritable k = new IntWritable(Integer.parseInt(key.toString()));
            IntWritable v = new IntWritable(Integer.parseInt(value.toString()));
            context.write(k, v);
        }
    }

    public static class SortReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            IntWritable value = values.iterator().next();
            context.write(key, value);
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
        int reducersNum = Integer.parseInt(conf.get("my.reducers"));
        sortJob.setNumReduceTasks(reducersNum);
        // Input
        sortJob.setInputFormatClass(KeyValueTextInputFormat.class);
        KeyValueTextInputFormat.addInputPath(sortJob, new Path(args[0]));
        sortJob.addCacheFile(new URI(samplesPath + samplesFile)); // TODO: add hdfs:// to uri
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
