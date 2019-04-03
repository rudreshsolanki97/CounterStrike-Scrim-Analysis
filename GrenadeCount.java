
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.commons.lang.StringUtils;
import org.apache.directory.api.util.Strings;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


// MATCHES Played per LEAGUE
// Total Matches per YEAR
// Max LENGTH of tournament between two TEAM
// Given a league, find wins for a particular team
// Given a year, find wins for a particular team

public class GrenadeCount {

    public static class Map extends
            Mapper<LongWritable, Text, Text, IntWritable> {
        private Text word = new Text();
        private IntWritable one = new IntWritable(1);
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line,",");
            String token="";
            int index=0;
            while (index<8) {
            	token = tokenizer.nextToken();
            	index++;
            }
            if (token.equals("Team 1") || token.equals("Team 2")) {
            	context.write(new Text(token), one);	
            }        
            }            
            }
        
        

    
    public static class Reduce extends
            Reducer<Text, IntWritable, Text, IntWritable> {   
        
        public void reduce(Text key, Iterable<IntWritable> values,
                Context context) throws IOException, InterruptedException {
        	int sum=0;
// For Printing SUM (5)        	
        	for (IntWritable value : values) {
        		sum += 1;
        	}        	
        	context.write(key, new IntWritable(sum));
    }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "matchinfo");
        job.setJarByClass(GrenadeCount.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path("/home/rudresh/eclipse-workspace/CSGo/mm_grenades_demos.csv"));
        FileOutputFormat.setOutputPath(job, new Path("/home/rudresh/eclipse-workspace/CSGo/output_grenadeCount"));

        job.waitForCompletion(true);
    }

}


