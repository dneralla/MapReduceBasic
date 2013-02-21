import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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

public class WordCounter {

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, IntWritable> {

		 private final static IntWritable one = new IntWritable(1); 
		private Text word = new Text();
		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {

			StringTokenizer itr = new StringTokenizer(value.toString());

			
				while (itr.hasMoreTokens()) {
					 word.set(itr.nextToken());
					 output.collect(word, one);
				}
			
		}

	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, Text> {

		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			int count = 0;
			while (values.hasNext()) {
				count += values.next().get();
			}
			output.collect(key, new Text("" + count));
		}

	}

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(WordCounter.class);
                conf.setJobName("word_counter");
        	conf.set("mapred.textoutputformat.separator","-");


                conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

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
