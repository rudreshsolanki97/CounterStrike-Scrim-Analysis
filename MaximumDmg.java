
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

public class MaximumDmg {
	public static String maxKeyOne="";
	public static int maxOne=-1;
	public static String maxKeyTwo="";
	public static int maxTwo=-1;

    public static class Map extends
            Mapper<LongWritable, Text, Text, IntWritable> {

        
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = line.split(",");
            String team=tokens[7], player = tokens[9];
        	int HPdmg = Integer.parseInt(tokens[13]), ARMdmg = Integer.parseInt(tokens[14]);
            System.out.println(HPdmg+ARMdmg);    
            if (team.equals("Team 1")||team.equals("Team 2")){
            context.write(new Text(team+" "+player), new IntWritable(HPdmg+ARMdmg));
            }
        }
        }

    
    public static class Reduce extends
            Reducer<Text, IntWritable, Text, IntWritable> {

    	
        public void reduce(Text key, Iterable<IntWritable> values,
                Context context) throws IOException, InterruptedException {

        	for (IntWritable value : values) {
        		if (key.toString().contains("Team 1")) {
        			if (value.get()>maxOne) {
            			maxOne=value.get();
            			maxKeyOne=key.toString();
            		}	
        		}else {
        			if (value.get()>maxTwo) {
            			maxTwo=value.get();
            			maxKeyTwo=key.toString();
            		}
        		}
        		
        	}
        }
        
        
        @Override 
        protected void cleanup(Context context) throws IOException,InterruptedException {
        	context.write(new Text(maxKeyOne),new IntWritable(maxOne));
        	context.write(new Text(maxKeyTwo),new IntWritable(maxTwo));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "matchinfo");
        job.setJarByClass(MaximumDmg.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path("/home/rudresh/eclipse-workspace/CSGo/mm_grenades_demos.csv"));
        FileOutputFormat.setOutputPath(job, new Path("/home/rudresh/eclipse-workspace/CSGo/maximum_dmg"));

        job.waitForCompletion(true);
    }

}

 


