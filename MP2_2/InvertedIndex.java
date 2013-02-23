import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class InvertedIndex {

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, IntWritable> {

		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {

			StringTokenizer itr = new StringTokenizer(value.toString());

			if (itr.hasMoreTokens()) {
				// first token = doc#
				IntWritable keyLine = new IntWritable(Integer.parseInt(itr.nextToken()));

				while (itr.hasMoreTokens()) {
					output.collect(new Text(itr.nextToken()), keyLine);
				}
			}
		}

	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, Text> {

		public class MySortedMapWritable extends SortedMapWritable {
	
		@Override
		public String toString() {
		
			Iterator itr = super.keySet().iterator();
			StringBuilder sb = new StringBuilder();
			
			while (itr.hasNext()) {
				
				IntWritable key = (IntWritable) itr.next();
				IntWritable val = (IntWritable) get(key);
				
				sb.append(key+"-"+val+" ");
			}
			
			return sb.toString();
		}
	}
			

		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, Text> output,
				Reporter reporter) throws IOException {
				
			
		MySortedMapWritable stat = new MySortedMapWritable();
		
			while (values.hasNext()) {

				IntWritable fileId = new IntWritable(values.next().get());
				IntWritable val;
				if (stat.containsKey(fileId)) {
					val = (IntWritable) stat.get(fileId);
					val.set(val.get() + 1);
					stat.put(fileId, val);
				} else {
					stat.put(fileId, new IntWritable(1));
				}
			}
			
			
			output.collect(key, new Text(stat.toString()));
		}

	}
	


	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(InvertedIndex.class);

		conf.setJobName("inverted_index");

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		// conf.setInputPath(new Path("src"));
		// conf.setOutputPath(new Path("out"));

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(
				args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(
				args[1]));

		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);

		client.setConf(conf);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
