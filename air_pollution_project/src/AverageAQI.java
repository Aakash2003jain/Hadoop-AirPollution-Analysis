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

public class AverageAQI {

    // Mapper Class
    public static class AQIMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            
            // Skip the header line
            if (line.contains("Daily AQI Value")) {
                return;
            }
    
            String[] fields = line.split(",");
            try {
                // Assuming the state is in the 16th column 
                String state = fields[16].trim(); 
                
                // Assuming AQI value is in the 6th column
                double aqiValue = Double.parseDouble(fields[6].trim()); 
                
                // Write state as key and AQI value as the value
                context.write(new Text(state), new DoubleWritable(aqiValue));
            } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
                // Log and skip invalid rows
                System.err.println("Invalid record: " + line);
            }
        }
    }
    

    // Reducer Class
    public static class AQIReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;
            // Sum up all AQI values for each state
            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }
            // Calculate and write the average AQI value
            if (count > 0) {
                context.write(key, new DoubleWritable(sum / count));
            }
        }
    }

    // Driver Code
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: AverageAQI <input path> <output path>");
            System.exit(-1);
        }

        // Set up the Hadoop configuration and job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Average AQI Calculation");
        job.setJarByClass(AverageAQI.class);

        // Set the mapper and reducer classes
        job.setMapperClass(AQIMapper.class);
        job.setReducerClass(AQIReducer.class);

        // Set the output key and value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // Set the input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Wait for the job to complete and exit
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
