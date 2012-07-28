package ca.uwaterloo.cpami.css.greedy.mapreduce.rowpersample;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Driver {

	public void run(String originalDataFile, String tempSelectionFile,
			int numPartitions, String selectedColumnsFile, int numColumns,
			int k, float l) throws IOException, InterruptedException,
			ClassNotFoundException {
		getPartitionSelectionJob(originalDataFile, tempSelectionFile,
				numPartitions, numColumns, k, l).waitForCompletion(true);

		getFinalSelectionJob(tempSelectionFile, selectedColumnsFile, k)
				.waitForCompletion(true);
	}

	private Job getPartitionSelectionJob(String originalDataFile,
			String tempSelectionFile, int numPartitions, int numColumns, int k,
			float l) throws IOException {
		Configuration config = new Configuration();
		config.setInt("partitionSubsetSize",
				(int) (k / numPartitions * (1 + l)));
		config.setInt("numPartitions", numPartitions);
		config.setInt("numColumns", numColumns);

		Job job = new Job(config);
		job.setJarByClass(Driver.class);
		FileInputFormat.addInputPaths(job, originalDataFile);
		FileOutputFormat.setOutputPath(job, new Path(tempSelectionFile));
		job.setMapperClass(PartitionSelectionMapper.class);
		job.setReducerClass(PartitionSelectionReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(DoubleArrayWritable.class);
		job.setMapOutputValueClass(SamplePartition.class);
		job.setNumReduceTasks(numPartitions);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		return job;
	}

	private Job getFinalSelectionJob(String tempSelectionFile,
			String selectedColumnsFile, int k) throws IOException {
		Configuration config = new Configuration();
		config.setInt("subsetSize", k);
		Job job = new Job(config);
		job.setJarByClass(Driver.class);
		FileInputFormat.addInputPaths(job, tempSelectionFile);
		FileOutputFormat.setOutputPath(job, new Path(selectedColumnsFile));
		job.setMapperClass(FinalSelectionMapper.class);
		job.setReducerClass(FinalSelectionReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(ArrayWritable.class);
		job.setMapOutputValueClass(SelectedColumn.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(1);
		return job;
	}

	/**
	 * 
	 * @param args
	 *            : args[0]: original Data File, args[1]: temp Selection File,
	 *            args[2]: numPartitions, args[3]: selected Columns File,
	 *            args[4]: total number of columns, args[5]: k, args[6]: l
	 *            (ratio 0 <= l <= 1)
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		new Driver().run(args[0], args[1], Integer.parseInt(args[2]), args[3],
				Integer.parseInt(args[4]), Integer.parseInt(args[5]),
				Float.parseFloat(args[6]));
	}

}