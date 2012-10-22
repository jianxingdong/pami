package ca.uwaterloo.cpami.css.greedy.mapreduce.rowpersample.onepass;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.math.VectorWritable;

// operates on the transpose (select from the rows)
public class GCSSOnePassDriver {

	/**
	 * 
	 * @param minSplitSize
	 *            : to be used to make sure there are enough columns at each
	 *            mapper
	 * @param maxSplitSize
	 *            : to be used to make sure the matrix at each column fits in
	 *            the mapper's memory
	 * @return: isSuccessful
	 */
	public boolean run(String originalDataFile, int k, int kSplit,
			long minSplitSize, long maxSplitSize, String selectedColumnsFile,
			Configuration conf) throws IOException, InterruptedException,
			ClassNotFoundException {
		FileSystem fs = FileSystem.get(conf);
		Path inputPath = new Path(originalDataFile);
		FileStatus fStatus = fs.getFileStatus(inputPath);
		System.out.println("block size: " + fStatus.getBlockSize());
		System.out.println("file len: " + fStatus.getLen());
		/*
		 * int numColsPerSplit = (int) Math.ceil(((1 + l) * k) /
		 * (fStatus.getLen() / minSplitSize));
		 */
		int numColsPerSplit = kSplit;
		conf.setInt("splitSubsetSize", numColsPerSplit);
		conf.setInt("subsetSize", k);

		Job job = new Job(conf);

		job.setJarByClass(GCSSOnePassDriver.class);
		FileInputFormat.addInputPath(job, inputPath);
		FileInputFormat.setMinInputSplitSize(job, minSplitSize);
		FileInputFormat.setMaxInputSplitSize(job, maxSplitSize);
		job.setInputFormatClass(AllRowsInputFormat.class);

		FileOutputFormat.setOutputPath(job, new Path(selectedColumnsFile));
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setMapperClass(GCSSOnePassMapper.class);
		job.setReducerClass(GCSSOnePassReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(VectorWritable.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setNumReduceTasks(1);
		job.waitForCompletion(false);
		return job.isSuccessful();
	}

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		// boolean r = new GCSSOnePassDriver().run("orth/nips/orth", 100, 166,
		// 450560l, 67108864l, "/C/cols", new Configuration());
		boolean r = new GCSSOnePassDriver().run("orth/nips/orth", 100, 166,
				450560l, 450560l, "/C/cols", new Configuration());
		System.out.println("isSuccessful: " + r);
	}
}
