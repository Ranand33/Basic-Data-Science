// ============================================================================
// 1. JAVA MAPREDUCE - NATIVE HADOOP APPROACH
// ============================================================================

// WordCount.java - Classic MapReduce example
import java.io.IOException;
import java.util.StringTokenizer;

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

public class WordCount {

    public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            
            // Convert to lowercase and remove punctuation
            String line = value.toString().toLowerCase().replaceAll("[^a-zA-Z0-9\\s]", "");
            StringTokenizer tokenizer = new StringTokenizer(line);
            
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) 
                throws IOException, InterruptedException {
            
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

// ============================================================================
// SalesAnalysis.java - Advanced MapReduce for business data
// ============================================================================

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

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

public class SalesAnalysis {

    public static class SalesMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        
        private Text category = new Text();
        private DoubleWritable sales = new DoubleWritable();
        private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

        public void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            
            String[] fields = value.toString().split(",");
            
            // Expected format: date,product,category,quantity,price
            if (fields.length == 5) {
                try {
                    String dateStr = fields[0];
                    String productCategory = fields[2];
                    double quantity = Double.parseDouble(fields[3]);
                    double price = Double.parseDouble(fields[4]);
                    
                    // Calculate total sales
                    double totalSales = quantity * price;
                    
                    // Filter for current month (example)
                    Date saleDate = dateFormat.parse(dateStr);
                    Date currentDate = new Date();
                    
                    category.set(productCategory);
                    sales.set(totalSales);
                    context.write(category, sales);
                    
                } catch (ParseException | NumberFormatException e) {
                    // Skip invalid records
                    context.getCounter("INVALID", "RECORDS").increment(1);
                }
            }
        }
    }

    public static class SalesReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) 
                throws IOException, InterruptedException {
            
            double sum = 0.0;
            int count = 0;
            
            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }
            
            result.set(sum);
            context.write(key, result);
            
            // Write additional statistics
            context.write(new Text(key + "_count"), new DoubleWritable(count));
            context.write(new Text(key + "_average"), new DoubleWritable(sum / count));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        
        // Set custom configuration
        conf.set("mapreduce.job.reduces", "4");
        conf.set("mapreduce.map.memory.mb", "2048");
        conf.set("mapreduce.reduce.memory.mb", "4096");
        
        Job job = Job.getInstance(conf, "sales analysis");
        job.setJarByClass(SalesAnalysis.class);
        job.setMapperClass(SalesMapper.class);
        job.setReducerClass(SalesReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

