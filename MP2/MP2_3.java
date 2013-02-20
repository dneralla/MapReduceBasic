import java.io.IOException;
import java.util.*;
import java.math.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.fs.Path;


public class MP2_3 {
	
	 public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		 
		    private Text galaxy = new Text();
		    
		    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		    {
		    	
		        String line = value.toString();
		        String [] tokens = line.split(":");
		        String []galName = tokens[1].split(" ");
		        double sum = 0;
		        long totalMeasures = 0;
		        for (int i = 2; i < tokens.length; i++) {
		        	String [] subMeasures = tokens[i].split(" ");
		        	for (int j = 0; j < subMeasures.length - 1; j++) { // -1 because it may includes one label
		        		if (!subMeasures[j].isEmpty()) 
		        		{
		        			sum += Long.parseLong(subMeasures[j]);
		        			totalMeasures++;
		        		}
		        	}
		        	
		        	if (i == tokens.length -1) {
		        		sum += Long.parseLong(subMeasures[subMeasures.length - 1]);
		        		totalMeasures++;
		        	}
		        	
		        	String [] label = tokens[i - 1].split(" ");
		        	galaxy.set(galName[1]);
		        	context.write(galaxy, new Text( label[label.length - 1] + "avg: " + Math.floor((sum/totalMeasures)*100)/100 + " "));
		       }
		 } 
	 }
	 
	 public static class Reduce extends Reducer<Text, Text, Text, Text> 
	 {

		    public void reduce(Text key, Iterable<Text> values, Context context) 
		      throws IOException, InterruptedException 
		      {
		    	SortedSet<String> x = new TreeSet<String>();
		        String aqum2 = new String();
		        for (Text val : values) 
		        {
		        	x.add(val.toString());
		        }   

		        Iterator i = x.iterator();
		        while(i.hasNext()) {
		        	aqum2 += i.next();
		        	
		        }
		        context.write(new Text("Galaxy: " + key), new Text(aqum2));
		      }
	 }
	 
	 
	public static void main(String[] args) throws IOException {
	    Configuration conf = new Configuration();
	    
	    conf.set("mapred.textoutputformat.separator"," ");
	    
	    Job job = new Job(conf, "MP2_3");
	   
	    
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);

	    job.setOutputKeyClass(Text.class);
	 	job.setOutputValueClass(Text.class);

	 	job.setJarByClass(MP2_3.class);       
	 	job.setMapperClass(Map.class);
	 	job.setReducerClass(Reduce.class);
	 	        
	 	job.setInputFormatClass(TextInputFormat.class);
	 	job.setOutputFormatClass(TextOutputFormat.class);
	        
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    try {
	     job.waitForCompletion(true);
	    } catch (InterruptedException e){
	    	System.err.println(e.getMessage());
	    } catch (ClassNotFoundException e) {
		System.err.println(e.getMessage());
	    }

	}
}
