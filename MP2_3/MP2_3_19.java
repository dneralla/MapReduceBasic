import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
//import org.apache.hadoop.mapreduce.*;
//import org.apache.hadoop.mapreduce.lib.input.*;
//import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;

public class MP2_3_19 extends Configured {

	 public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		 
		    private Text galaxy = new Text();
		        
		    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException 
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
		        	output.collect(galaxy, new Text( label[label.length - 1] + "avg: " + Math.floor((sum/totalMeasures)*100)/100 + " "));
		       }
		        
		 } 
	 }
	 
	 public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> 
	 {

		    public void reduce(Text key, Iterator<Text> values,  OutputCollector<Text, Text> output,  Reporter reporter) 
		      throws IOException 
		      {
		    	SortedSet<String> x = new TreeSet<String>();
		        String aqum2 = new String();
		        while(values.hasNext()) 
		        {
		        	x.add(values.next().toString());
		        }   

		        Iterator i = x.iterator();
		        while(i.hasNext()) {
		        	aqum2 += i.next();
		        	
		        }
		        output.collect(new Text("Galaxy: " + key), new Text(aqum2));
		    }
		 }
	 
	 
	public static void main(String[] args) throws IOException 
	{
	    
		Configuration conf = new Configuration();
		
		conf.set("mapred.textoutputformat.separator"," ");
		
		JobConf job = new JobConf( conf, MP2_3_19.class);
		job.setJobName("MP2_3");
		
		 job.setMapOutputKeyClass(Text.class);
		 job.setMapOutputValueClass(Text.class);
		
		 job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		job.setJarByClass(MP2_3_19.class);       
	 	job.setMapperClass(Map.class);
	 	job.setReducerClass(Reduce.class);
		
	 	FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	 	
	    JobClient.runJob(job);
	    System.exit(0);
		/*   
	    Job job = new Job(conf, "MP2_3");
	    
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(LongArrayWritable.class);

	    job.setOutputKeyClass(Text.class);
	 	job.setOutputValueClass(LongWritable.class);

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
	    */
	}
}
