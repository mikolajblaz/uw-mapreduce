import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * Created by mikib on 23.11.16.
 */

public class TeraSort extends Configured implements Tool {

    private final static String samplesPath = "output_samples/";
    private final static String samplesFile = "part-r-00000";
    private final static String sortedPath = "output_sorted/";
    private final static String rankedPath = "output_ranked/";
    private final static String perfectPath = "output_perfect/";

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
            globalValue.set(intIndex, intValue);
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
    public static class RankMapper extends Mapper<Text, Text, IntWritable, PairInt>{
        protected IntWritable globalReducer = new IntWritable();
        protected PairInt globalValue = new PairInt();

        @Override
        protected void map(Text key, Text val, Context context) throws IOException, InterruptedException {
            String[] indVal = val.toString().split("\t");
            int index = Integer.parseInt(indVal[0]);
            int value = Integer.parseInt(indVal[1]);

            globalReducer.set(Integer.parseInt(key.toString()));
            globalValue.set(index, value);
            context.write(globalReducer, globalValue);
        }
    }

    public static class RankReducer extends Reducer<IntWritable, PairInt, IntWritable, PairInt> {
        @Override
        protected void reduce(IntWritable key, Iterable<PairInt> values, Context context) throws IOException, InterruptedException {
            List<PairInt> sortedValues = new ArrayList<>();
            for (PairInt val : values)
                sortedValues.add(new PairInt(val));
            Collections.sort(sortedValues);

            IntWritable rank = new IntWritable();

            int prefixCount = 0;
            for (PairInt val : sortedValues) {
                if (val.getFirst() < 0) {   // count prefix from all preceding machines
                    System.out.println(val);
                    prefixCount += val.getSecond();
                } else {
                    rank.set(prefixCount);
                    context.write(rank, val);
                    prefixCount++;
                }
            }
        }
    }

    /* ########################################## Perfect Sort ############################################## */
    public static class PerfectMapper extends Mapper<Text, Text, IntWritable, TripleInt>{
        protected IntWritable globalReducer = new IntWritable();
        protected TripleInt globalValue = new TripleInt();

        protected double m; // number of records on one balanced machine

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // all records
            int n = Integer.parseInt(context.getConfiguration().get("my.records"));
            int reducersNum = context.getNumReduceTasks();
            m = Math.ceil(n / (double) reducersNum);
        }

        @Override
        protected void map(Text key, Text val, Context context) throws IOException, InterruptedException {
            String[] indVal = val.toString().split("\t");
            int rank = Integer.parseInt(key.toString());
            int index = Integer.parseInt(indVal[0]);
            int value = Integer.parseInt(indVal[1]);

            int reducer = (int) Math.ceil((rank + 1) / m) - 1;
            globalReducer.set(reducer);
            globalValue.set(rank, index, value);
            context.write(globalReducer, globalValue);
        }
    }

    public static class PerfectReducer extends Reducer<IntWritable, TripleInt, IntWritable, TripleInt> {
        // count reducers, for which current objects are remotely relevant
        protected int[] remotelyRelevantReducers(int currReducer) {
            // TODO: add remote reducers
            return new int[]{currReducer};
        }

        @Override
        protected void reduce(IntWritable key, Iterable<TripleInt> values, Context context) throws IOException, InterruptedException {
            List<TripleInt> sortedValues = new ArrayList<>();
            for (TripleInt val : values)
                sortedValues.add(new TripleInt(val));
            Collections.sort(sortedValues);

            int reducersNum = context.getNumReduceTasks();
            int currReducer = key.get();
            int[] remoteReducers = remotelyRelevantReducers(currReducer);

            IntWritable reducer = new IntWritable();
            int prefixAggregate = 0;

            // send all machine objects to self and at most 2 remotely relevant machines
            for (TripleInt val : sortedValues) {
                for (Integer red : remoteReducers) {
                    reducer.set(red);
                    context.write(reducer, val);
                }

                prefixAggregate += val.getThird();
            }

            // send aggregates to all machines
            TripleInt globalValue = new TripleInt(- currReducer - 1, - currReducer - 1, prefixAggregate);
            for (int red = 0; red < reducersNum; red++) {
                reducer.set(red);
                context.write(reducer, globalValue);
            }
            // TODO: remember about adding 1 to reducer number ^^^^^
        }
    }

    /* ########################################## Sliding Aggregation ############################################## */
    public static class AggrMapper extends Mapper<Text, Text, IntWritable, TripleInt>{
        protected IntWritable globalReducer = new IntWritable();
        protected TripleInt globalValue = new TripleInt();

        protected double m; // number of records on one balanced machine

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // all records
            int n = Integer.parseInt(context.getConfiguration().get("my.records"));
            int reducersNum = context.getNumReduceTasks();
            m = Math.ceil(n / (double) reducersNum);
        }

        @Override
        protected void map(Text key, Text val, Context context) throws IOException, InterruptedException {
            String[] rankIndVal = val.toString().split("\t");
            int rank = Integer.parseInt(rankIndVal[0]);
            int index = Integer.parseInt(rankIndVal[1]);
            int value = Integer.parseInt(rankIndVal[2]);

            int reducer = Integer.parseInt(key.toString());
            globalReducer.set(reducer);
            globalValue.set(rank, index, value);
            context.write(globalReducer, globalValue);
        }
    }

    public static class AggrReducer extends Reducer<IntWritable, TripleInt, IntWritable, TripleInt> {
        @Override
        protected void reduce(IntWritable key, Iterable<TripleInt> values, Context context) throws IOException, InterruptedException {
        }
    }


    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        int reducersNum = Integer.parseInt(conf.get("my.reducers"));
        conf.set("mapred.reduce.tasks", Integer.toString(reducersNum)); // TODO: needed?
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

        if (!sampleJob.waitForCompletion(true))
            return 1;

        /* ################# Sorting ################## */
        Job sortJob = Job.getInstance(conf, "TeraSort sorting");
        sortJob.setJarByClass(TeraSort.class);
        // Map-Combine-Reduce
        sortJob.setMapperClass(SortMapper.class);
        sortJob.setReducerClass(SortReducer.class);
        // Input
        sortJob.setInputFormatClass(KeyValueTextInputFormat.class);
        KeyValueTextInputFormat.addInputPath(sortJob, new Path(args[0]));
        sortJob.addCacheFile(new URI(samplesPath + samplesFile)); // TODO: add hdfs:// to uri
        // Output
        sortJob.setOutputKeyClass(IntWritable.class);
        sortJob.setOutputValueClass(PairInt.class);
        FileOutputFormat.setOutputPath(sortJob, new Path(sortedPath));

        if (!sortJob.waitForCompletion(true))
            return 1;

        /* ################# Ranking ################## */
        Job rankJob = Job.getInstance(conf, "TeraSort ranking");
        rankJob.setJarByClass(TeraSort.class);
        // Map-Combine-Reduce
        rankJob.setMapperClass(RankMapper.class);
        rankJob.setReducerClass(RankReducer.class);
        // Input
        rankJob.setInputFormatClass(KeyValueTextInputFormat.class);
        KeyValueTextInputFormat.addInputPath(rankJob, new Path(sortedPath));
        // Output
        rankJob.setOutputKeyClass(IntWritable.class);
        rankJob.setOutputValueClass(PairInt.class);
        FileOutputFormat.setOutputPath(rankJob, new Path(rankedPath));

        if (!rankJob.waitForCompletion(true))
            return 1;

        // get number of objects:
        Counters counters = rankJob.getCounters();
        Counter outputRecordsCounter = counters.findCounter("org.apache.hadoop.mapreduce.TaskCounter", "REDUCE_OUTPUT_RECORDS");
        int recordsNum = (int) outputRecordsCounter.getValue();

        /* ################# Perfect Sort ################## */
        conf.set("my.records", Integer.toString(recordsNum));

        Job perfectJob = Job.getInstance(conf, "TeraSort perfect sort");
        perfectJob.setJarByClass(TeraSort.class);
        // Map-Combine-Reduce
        perfectJob.setMapperClass(PerfectMapper.class);
        perfectJob.setReducerClass(PerfectReducer.class);
        // Input
        perfectJob.setInputFormatClass(KeyValueTextInputFormat.class);
        KeyValueTextInputFormat.addInputPath(perfectJob, new Path(rankedPath));
        // Output
        perfectJob.setOutputKeyClass(IntWritable.class);
        perfectJob.setOutputValueClass(TripleInt.class);
        FileOutputFormat.setOutputPath(perfectJob, new Path(perfectPath));

        if (!perfectJob.waitForCompletion(true))
            return 1;


        /* ################# Sliding Aggregation ################## */
        Job aggrJob = Job.getInstance(conf, "Sliding Aggregation");
        aggrJob.setJarByClass(TeraSort.class);
        // Map-Combine-Reduce
        aggrJob.setMapperClass(AggrMapper.class);
        aggrJob.setReducerClass(AggrReducer.class);
        // Input
        aggrJob.setInputFormatClass(KeyValueTextInputFormat.class);
        KeyValueTextInputFormat.addInputPath(aggrJob, new Path(perfectPath));
        // Output
        aggrJob.setOutputKeyClass(IntWritable.class);
        aggrJob.setOutputValueClass(TripleInt.class);
        FileOutputFormat.setOutputPath(aggrJob, new Path(args[1]));

        if (!aggrJob.waitForCompletion(true))
            return 1;

        // final job:
        // FileOutputFormat.setOutputPath(finalJob, new Path(args[1]));
        return 0;
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
