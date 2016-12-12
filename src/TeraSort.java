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

    /* ########################################## Sorter ############################################## */
    public static class SortMapper extends Mapper<Text, Text, IntWritable, PairInt>{

        protected int[] borders;
        protected IntWritable globalReducer = new IntWritable();
        protected PairInt globalValue = new PairInt();

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

        protected int find_border(int index) {
            int j = 1;
            while (index > borders[j]) {
                j++;
            }
            return j - 1;
        }

        @Override
        public void map(Text index, Text value, Context context) throws IOException, InterruptedException {
            int intIndex = Integer.parseInt(index.toString());
            int intValue = Integer.parseInt(value.toString());

            int reducer = find_border(intIndex);
            globalReducer.set(reducer);
            globalValue.setFirst(intIndex);
            globalValue.setSecond(intValue);

            context.write(globalReducer, globalValue);
        }
    }

    public static class SortReducer extends Reducer<IntWritable, PairInt, IntWritable, PairInt> {
        @Override
        public void reduce(IntWritable key, Iterable<PairInt> values, Context context) throws IOException, InterruptedException {
            List<PairInt> sortedValues = new ArrayList<>();
            for (PairInt val : values)
                sortedValues.add(new PairInt(val));
            Collections.sort(sortedValues);

            for (PairInt v : sortedValues)
                context.write(key, v);

            int reducersNum = context.getNumReduceTasks();
            int currReducer = key.get();

            // Send machine count to all machines (in next MR phase)
            PairInt allCount = new PairInt(-1, sortedValues.size());
            IntWritable red = new IntWritable();
            for (int reducer = currReducer + 1; reducer < reducersNum; reducer++) {
                red.set(reducer);
                context.write(red, allCount);
            }
        }
    }

    /* ########################################## Ranking ############################################## */

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        /* ################# Samples ################## */
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

        /* ################# Sorting ################## */
        Job sortJob = Job.getInstance(conf, "TeraSort sorting");
        sortJob.setJarByClass(TeraSort.class);
        // Map-Combine-Reduce
        sortJob.setMapperClass(SortMapper.class);
        sortJob.setReducerClass(SortReducer.class);
        int reducersNum = Integer.parseInt(conf.get("my.reducers"));
        sortJob.setNumReduceTasks(reducersNum);
        // Input
        sortJob.setInputFormatClass(KeyValueTextInputFormat.class);
        KeyValueTextInputFormat.addInputPath(sortJob, new Path(args[0]));
        sortJob.addCacheFile(new URI(samplesPath + samplesFile)); // TODO: add hdfs:// to uri
        // Output
        sortJob.setOutputKeyClass(IntWritable.class);
        sortJob.setOutputValueClass(PairInt.class);
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
