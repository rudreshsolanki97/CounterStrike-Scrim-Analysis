
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
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

public class PlayerValue {
	

    public static class Map extends
            Mapper<LongWritable, Text, Text, DoubleWritable> {
        private Text word = new Text();
        private DoubleWritable one = new DoubleWritable(1);
        
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = line.split(",");
        	int HPdmg = Integer.parseInt(tokens[13]), ARMdmg = Integer.parseInt(tokens[14]);
            if (tokens[7].trim().equals("Team 1")||tokens[7].trim().equals("Team 2")) {
            	context.write(new Text(tokens[9]), new DoubleWritable(HPdmg));
            }
            }            
            }
        
        

    
    public static class Reduce extends
            Reducer<Text, DoubleWritable, Text, DoubleWritable> {   

    	public static int count=0;
        public static HashMap<String,Integer> playerDMG = new HashMap<String,Integer>();
        public static HashMap<String,Integer> playerGrenadeCount = new HashMap<String,Integer>();

        public void reduce(Text key, Iterable<DoubleWritable> values,
                Context context) throws IOException, InterruptedException {
        	int totalDMG=0;
        	int totalThrow=0;
        	for (DoubleWritable value : values) {
        		totalDMG += value.get();
        		totalThrow++;
        	}        	
        	playerDMG.put(key.toString(), totalDMG);
        	playerGrenadeCount.put(key.toString(),totalThrow);
    }
        @Override
        protected void cleanup(Context context)  throws IOException,InterruptedException {
        	for(String key :playerDMG.keySet()){
        		   System.out.println(key+"\t"+playerDMG.get(key)+"\t"+playerGrenadeCount.get(key));        		   
        		if (playerGrenadeCount.get(key)!=0) {
        		   double dmgPercent = (double)playerDMG.get(key)*100/(double)(playerGrenadeCount.get(key));
         		   System.out.println(dmgPercent);
         	       context.write(new Text(key+"\t"+playerGrenadeCount.get(key)+"\t"+playerDMG.get(key)),new DoubleWritable(dmgPercent)); 	
        		}
        	   }  
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "matchinfo");
        job.setJarByClass(PlayerValue.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path("/home/rudresh/data/Workspace/BigData/CounterStrike-Scrim-Analysis/mm_grenades_demos.csv"));
        File f = new File("/home/rudresh/data/Workspace/BigData/CounterStrike-Scrim-Analysis/output_grenadeCount");
        if (f.exists()) {
        	f.delete();
        }
        FileOutputFormat.setOutputPath(job, new Path("/home/rudresh/data/Workspace/BigData/CounterStrike-Scrim-Analysis/output_grenadeCount"));
        job.waitForCompletion(true);
    }

}


