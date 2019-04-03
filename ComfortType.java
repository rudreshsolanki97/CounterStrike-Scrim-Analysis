import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ComfortType {

    public static class Map extends
            Mapper<LongWritable, Text, Text, DoubleWritable> {
        private Text word = new Text();
        private DoubleWritable one = new DoubleWritable(1);
        private DoubleWritable zero = new DoubleWritable(0);

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = line.split(",");
        	int HPdmg = Integer.parseInt(tokens[13]), ARMdmg = Integer.parseInt(tokens[14]);
            if ((tokens[7].trim().equals("Team 1")||tokens[7].trim().equals("Team 2"))&&tokens[18].equals("HE"))  {
            	if (tokens[17].length()<7) {
            		context.write(new Text(tokens[9]), zero);
            	}
            	else{
            		context.write(new Text(tokens[9]), one);
            	}
            }
            }            
            }
        
        

    
    public static class Reduce extends
            Reducer<Text, DoubleWritable, Text, DoubleWritable> {   
    	
        public void reduce(Text key, Iterable<DoubleWritable> values,
                Context context) throws IOException, InterruptedException {
        	double sum=0;
        	double totalThrow=0;
        	for (DoubleWritable value : values) {
        		sum += value.get();
        		totalThrow++;
        	}        	
        	context.write(key, new DoubleWritable(sum*100/totalThrow));
    }
        
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "matchinfo");
        job.setJarByClass(ComfortType.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path("/home/rudresh/data/Workspace/BigData/CounterStrike-Scrim-Analysis/mm_grenades_demos.csv"));        
        FileOutputFormat.setOutputPath(job, new Path("/home/rudresh/data/Workspace/BigData/CounterStrike-Scrim-Analysis/output_comfort"));
        job.waitForCompletion(true);
    }
}
