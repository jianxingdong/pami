package ca.uwaterloo.cpami.css.baselines;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class FirstKColumnsSelectionJob {

	public static class FirstKColsSelectionMapper extends
			Mapper<IntWritable, VectorWritable, IntWritable, VectorWritable> {

		private VectorWritable rowWritable = new VectorWritable();
		private int k;

		protected void setup(
				org.apache.hadoop.mapreduce.Mapper<IntWritable, VectorWritable, IntWritable, VectorWritable>.Context context)
				throws IOException, InterruptedException {
			k = context.getConfiguration().getInt("num-cols", 0);
		};

		protected void map(
				IntWritable key,
				VectorWritable value,
				org.apache.hadoop.mapreduce.Mapper<IntWritable, VectorWritable, IntWritable, VectorWritable>.Context context)
				throws java.io.IOException, InterruptedException {

			Vector fullVectr = value.get();
			RandomAccessSparseVector partialVector = new RandomAccessSparseVector(
					k);

			for (int i = 0; i < k; i++) {
				if (fullVectr.getQuick(i) != 0)
					partialVector.set(i, fullVectr.getQuick(i));
			}
			rowWritable.set(partialVector);
			// rowWritable.set(value.get().viewPart(0, k).clone()); //TODO does
			// not work for some reason
			context.write(key, rowWritable);
		};
	}

	public void selectFirstKCols(String matrixPath, int k, String outputPath)
			throws IOException, InterruptedException, ClassNotFoundException {

		final Configuration conf = new Configuration();
		conf.setInt("num-cols", k);
		Job job = new Job(conf);
		job.setJarByClass(FirstKColumnsSelectionJob.class);
		FileInputFormat.addInputPaths(job, matrixPath);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		job.setMapperClass(FirstKColsSelectionMapper.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(VectorWritable.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setNumReduceTasks(0);
		job.waitForCompletion(false);
		if (!job.isSuccessful())
			throw new RuntimeException("First K columns Job is unsuccessful");
	}

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		new RandomSelectionJob().runRandomSelection("/tmpfirstk9/",
				1000, 100, "/tmp3rnd");
		/*
		new FirstKColumnsSelectionJob().selectFirstKCols("orth/nips.dat", 1000,
				"/tmpfirstk9");
		final Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(
				"/tmpfirstk9/part-m-00000"), conf);
		IntWritable key = new IntWritable();
		VectorWritable val = new VectorWritable();
		reader.next(key, val);
		System.out.println(val.get().size());
		reader.close();
		*/

	}
}