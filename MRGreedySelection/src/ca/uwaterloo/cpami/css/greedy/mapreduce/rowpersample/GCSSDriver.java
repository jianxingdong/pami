package ca.uwaterloo.cpami.css.greedy.mapreduce.rowpersample;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.math.VectorWritable;

public class GCSSDriver {

	public boolean run(String originalDataFile, String tempSelectionFile,
			int numPartitions, String selectedColumnsFile, int numRows,
			int numColumns, int k, float l, int numReducers)
			throws IOException, InterruptedException, ClassNotFoundException {
		Job partitionSelectionJob = getPartitionSelectionJob(originalDataFile,
				tempSelectionFile, numPartitions, numRows, numColumns, k, l,
				numReducers);
		partitionSelectionJob.waitForCompletion(false);
		if (!partitionSelectionJob.isSuccessful())
			return false;

		Job finalSelectionJob = getFinalSelectionJob(tempSelectionFile,
				selectedColumnsFile, numRows, k);
		finalSelectionJob.waitForCompletion(false);
		return finalSelectionJob.isSuccessful();

	}

	private Job getPartitionSelectionJob(String originalDataFile,
			String tempSelectionFile, int numPartitions, int numRows,
			int numColumns, int k, float l, int numReducers) throws IOException {
		Configuration config = new Configuration();
		config.setInt("subsetSize",
				(int) Math.ceil(((1 + l) * k / numPartitions)));
		config.setInt("numPartitions", numPartitions);
		config.setInt("numColumns", numColumns);
		config.setInt("numRows", numRows);

		Job job = new Job(config);
		job.setJarByClass(GCSSDriver.class);
		FileInputFormat.addInputPaths(job, originalDataFile);

		FileOutputFormat.setOutputPath(job, new Path(tempSelectionFile));
		job.setMapperClass(PartitionSelectionMapper.class);
		job.setReducerClass(PartitionSelectionReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(VectorWritable.class);
		job.setNumReduceTasks(numReducers);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		return job;
	}

	private Job getFinalSelectionJob(String tempSelectionFile,
			String selectedColumnsFile, int numRows, int k) throws IOException {
		Configuration config = new Configuration();
		config.setInt("subsetSize", k);
		config.setInt("numRows", numRows);
		Job job = new Job(config);
		job.setJarByClass(GCSSDriver.class);
		FileInputFormat.addInputPaths(job, tempSelectionFile);
		FileOutputFormat.setOutputPath(job, new Path(selectedColumnsFile));
		job.setMapperClass(FinalSelectionMapper.class);
		job.setReducerClass(FinalSelectionReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(VectorWritable.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setNumReduceTasks(1);
		return job;
	}

	public static void main(String[] args) {
		int numPartitions = 40;
		int k = 100;
		float l = 0.3f;
		System.out.println((int) Math.ceil(((1 + l) * k / numPartitions)));
	}
}