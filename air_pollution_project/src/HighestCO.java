import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HighestCO {

    // Mapper Class
    public static class COMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            // Skip the header (assumes header contains "CO" keyword)
            if (line.contains("CO")) {
                return;
            }

            // Split the line by commas
            String[] fields = line.split(",");

            try {
                // Assuming the state is in the 1st column (index 0) and CO value is in the 3rd column (index 2)
                String state = fields[16].trim(); // Adjust this index if needed
                double coValue = Double.parseDouble(fields[4].trim()); // Adjust this index for CO value

                // Emit the state as key and CO value as value
                context.write(new Text(state), new DoubleWritable(coValue));
            } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
                // Log and skip invalid rows
                System.err.println("Invalid line: " + line);
            }
        }
    }

    // Reducer Class
    public static class COReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double maxCO = Double.MIN_VALUE;

            // Find the maximum CO value for each state
            for (DoubleWritable val : values) {
                maxCO = Math.max(maxCO, val.get());
            }

            // Write the state and its maximum CO value
            context.write(key, new DoubleWritable(maxCO));
        }
    }

    // Main Method
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "highest CO concentration");
        job.setJarByClass(HighestCO.class);

        // Set Mapper and Reducer classes
        job.setMapperClass(COMapper.class);
        job.setReducerClass(COReducer.class);

        // Set Output Key and Value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // Set Input and Output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Wait for job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
