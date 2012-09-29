package ca.uwaterloo.cpami.css.baselines;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import ca.uwaterloo.cpami.mahout.matrix.utils.Helpers;

public class EMRHeap {

	public static class EMRHeapMapper extends
			Mapper<LongWritable, Text, LongWritable, Text> {

		protected void map(
				LongWritable key,
				Text value,
				org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {
			System.out.println("Max Memory: "
					+ Runtime.getRuntime().maxMemory());
			System.out.println("Free Memory: "
					+ Runtime.getRuntime().freeMemory());
			System.out.println("Total Memory: "
					+ Runtime.getRuntime().totalMemory());

			System.out.println("Num Maps"
					+ context.getConfiguration().get(
							"mapred.tasktracker.map.tasks.maximum"));
			System.out.println("Num Reduce"
					+ context.getConfiguration().get(
							"mapred.tasktracker.reduce.tasks.maximum"));

			context.write(key, value);

		};
	}

	public static class EMRHeapReducer extends
			Reducer<LongWritable, Text, LongWritable, Text> {

		protected void reduce(
				LongWritable arg0,
				java.lang.Iterable<Text> arg1,
				org.apache.hadoop.mapreduce.Reducer<LongWritable, Text, LongWritable, Text>.Context arg2)
				throws java.io.IOException, InterruptedException {
			System.out.println("Max Memory: "
					+ Runtime.getRuntime().maxMemory());
			System.out.println("Free Memory: "
					+ Runtime.getRuntime().freeMemory());
			System.out.println("Total Memory: "
					+ Runtime.getRuntime().totalMemory());

			System.out.println("Num Maps"
					+ arg2.getConfiguration().get(
							"mapred.tasktracker.map.tasks.maximum"));
			System.out.println("Num Reduce"
					+ arg2.getConfiguration().get(
							"mapred.tasktracker.reduce.tasks.maximum"));

		};
	}

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		final Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJarByClass(Helpers.class);
		FileInputFormat.addInputPaths(job, args[1]);
		FileOutputFormat.setOutputPath(job, new Path("/outputpath/P3"));
		job.setMapperClass(EMRHeapMapper.class);
		job.setReducerClass(EMRHeapReducer.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(2);
		job.waitForCompletion(false);
		if (!job.isSuccessful())
			throw new RuntimeException("Repartition Job is unsuccessful");

	}
}
