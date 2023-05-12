import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class IMDBStudent20200994 {

	public static class IMDBMapper extends Mapper<Object, Text, Text, LongWritable> {
		private final static LongWritable one = new LongWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// IMDB data는 id, title, genre 순
			// 1::Toy Story (1995)::Animation|Children's|Comedy
			String[] imdb = value.toString().split("::");
			if (imdb.length >= 3) { // genre가 포함된 데이터일 때만
				String genres = imdb[2]; // genre 
				StringTokenizer itr = new StringTokenizer(genres, "|");
				while (itr.hasMoreTokens()) {
					word.set(itr.nextToken());	
					context.write(word, one);
				}
			}
		}
	}

	public static class IMDBReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable result = new LongWritable();

		public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			long sum = 0;
			for (LongWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: IMDB <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "IMDB");
		job.setJarByClass(IMDBStudent20200994.class);
		job.setMapperClass(IMDBMapper.class);
		job.setCombinerClass(IMDBReducer.class);
		job.setReducerClass(IMDBReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

