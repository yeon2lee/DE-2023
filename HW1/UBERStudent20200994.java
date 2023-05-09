import java.io.*;
import java.util.*;
import java.time.DayOfWeek;
import java.time.LocalDate; 

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class UBERStudent20200994 {

	public static class UBERMapper extends Mapper<Object, Text, Text, Text> {
		private Text regionDay = new Text();
		private Text tripVehicle = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), ",");
			String baseNumber = itr.nextToken(); // region
			String tmpDate = itr.nextToken(); // date
        		String trips = itr.nextToken(); // trips
			String vehicles = itr.nextToken(); // vehicles

			// date to dayOfWeek
			StringTokenizer itr2 = new StringTokenizer(tmpDate, "/");
			int month = Integer.parseInt(itr2.nextToken());
			int day = Integer.parseInt(itr2.nextToken());
			int year = Integer.parseInt(itr2.nextToken());

			String[] days = {"MON", "TUE", "WED", "THR", "FRI", "SAT", "SUN"};
			LocalDate date = LocalDate.of(year, month, day);
			DayOfWeek dayOfWeek = date.getDayOfWeek();
			int dayOfWeekNumber = dayOfWeek.getValue();
		        String str = days[dayOfWeekNumber - 1];

			regionDay.set(baseNumber + "," + str);
			tripVehicle.set(trips + "," + vehicles);

			context.write(regionDay, tripVehicle);
		}
	}

	public static class UBERReducer extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			long trips = 0;
			long vehicles = 0;
			for (Text text : values) {
				String tripVehicle = text.toString();
				StringTokenizer itr = new StringTokenizer(tripVehicle, ",");
				trips += Long.parseLong(itr.nextToken());
				vehicles += Long.parseLong(itr.nextToken());
			}
			result.set(trips + "," + vehicles);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: UBER <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "UBER");
		job.setJarByClass(UBERStudent20200994.class);
		job.setMapperClass(UBERMapper.class);
		job.setCombinerClass(UBERReducer.class);
		job.setReducerClass(UBERReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

