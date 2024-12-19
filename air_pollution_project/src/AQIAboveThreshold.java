import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AQIAboveThreshold {
    public static class ThresholdMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private boolean isHeader = true; // Track if the current line is the header
        private static final int AQI_THRESHOLD = 10; // Replace with your threshold value

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            // Skip the header
            if (isHeader) {
                isHeader = false;
                return;
            }

            // Split the line by commas
            String[] fields = line.split(",");

            try {
                String city = fields[7].trim(); 
                int aqiValue = Integer.parseInt(fields[6].trim()); 

                // Emit if AQI is above the threshold
                if (aqiValue > AQI_THRESHOLD) {
                    context.write(new Text(city), new IntWritable(1)); // Emit 1 for each occurrence
                }
            } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
                // Log and skip invalid rows
                System.err.println("Invalid line: " + line);
            }
        }
    }

    public static class ThresholdReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int count = 0;

            // Count the number of occurrences where AQI exceeds the threshold
            for (IntWritable val : values) {
                count += val.get(); // Sum up the counts (each is 1)
            }

            // Write city and count of AQI above the threshold
            context.write(key, new IntWritable(count));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "AQI above threshold");
        job.setJarByClass(AQIAboveThreshold.class);
        job.setMapperClass(ThresholdMapper.class);
        job.setReducerClass(ThresholdReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
