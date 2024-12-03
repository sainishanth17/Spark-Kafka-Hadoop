import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class ReservoirSampling {

    // Mapper class
    public static class ReservoirSamplingMapper extends Mapper<Object, Text, Text, Text> {
        private static Random rand = new Random();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Pass each line to the reducer for sampling
            context.write(new Text("sample"), value);
        }
    }

    // Reducer class
    public static class ReservoirSamplingReducer extends Reducer<Text, Text, Text, Text> {
        private int k = 900; // Sample size (default 900)
        private Random rand = new Random();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Read the sample size from the configuration (can be passed from the command line)
            k = Integer.parseInt(context.getConfiguration().get("sample.size", "900"));
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> reservoir = new ArrayList<>(k);
            int count = 0;

            // Reservoir Sampling Logic
            for (Text value : values) {
                count++;
                if (count <= k) {
                    // Fill the reservoir until it has 'k' elements
                    reservoir.add(value.toString());
                } else {
                    // Replace elements in the reservoir with probability
                    int replaceIndex = rand.nextInt(count);
                    if (replaceIndex < k) {
                        reservoir.set(replaceIndex, value.toString());
                    }
                }
            }

            // Output the reservoir sample (k random lines)
            for (String sample : reservoir) {
                context.write(new Text(sample), new Text(""));
            }
        }
    }

    // Driver class
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: ReservoirSampling <input path> <output path> <sample size>");
            System.exit(-1);
        }

        String inputPath = args[0];
        String outputPath = args[1];
        int sampleSize = Integer.parseInt(args[2]);

        Configuration conf = new Configuration();
        conf.set("sample.size", String.valueOf(sampleSize));

        Job job = Job.getInstance(conf, "Reservoir Sampling");
        job.setJarByClass(ReservoirSampling.class);

        // Set Mapper and Reducer classes
        job.setMapperClass(ReservoirSamplingMapper.class);
        job.setReducerClass(ReservoirSamplingReducer.class);

        // Set the output types for Mapper and Reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set the input and output paths
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.setNumReduceTasks(1); 

        // Run the job and wait for completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
