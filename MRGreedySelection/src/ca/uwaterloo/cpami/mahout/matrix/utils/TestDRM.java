package ca.uwaterloo.cpami.mahout.matrix.utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.hadoop.DistributedRowMatrix;

public class TestDRM {

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		final Configuration conf = new Configuration();
		DistributedRowMatrix a = new DistributedRowMatrix(
				new Path("/lanc/BP3"), new Path("/tmpfix/B/B"), 100, 30);
		a.setConf(conf);
		Helpers.repartitionMatrix(new Path("/lanc/B"),
				new Path("/lanc/BPrep3"), 3);
		DistributedRowMatrix b = new DistributedRowMatrix(new Path(
				"/lanc/BPrep3"), new Path("/tmpfix/BP/BP"), 100, 30);
		b.setConf(conf);
		System.out.println(a.times(b).getRowPath());

	}

	public static void _main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {

		final Configuration conf = new Configuration();

		Job job = new Job(conf);
		job.setJarByClass(FrobeniusNormDiffJob.class);
		FileInputFormat.addInputPaths(job, "/lanc/B");
		FileOutputFormat.setOutputPath(job, new Path("/lanc/BP22"));
		job.setMapperClass(Mapper.class);
		job.setReducerClass(Reducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(VectorWritable.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setNumReduceTasks(2);
		job.waitForCompletion(true);
	}
}
