package ca.uwaterloo.cpami.css.baselines;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class RandomSelectionJob {

	public static class RandomSelectionMapper extends
			Mapper<IntWritable, VectorWritable, IntWritable, VectorWritable> {

		private List<Integer> selectedIndices = new ArrayList<Integer>();

		protected void setup(
				org.apache.hadoop.mapreduce.Mapper<IntWritable, VectorWritable, IntWritable, VectorWritable>.Context context)
				throws IOException, InterruptedException {			
			final Configuration conf = new Configuration();
			final FileSystem fs = FileSystem.get(conf);
			FSDataInputStream in = fs.open(new Path(context.getConfiguration()
					.get("selectedIndicesFile")));
			try {
				while (true) {
					selectedIndices.add(in.readInt());
				}
			} catch (EOFException eof) {
				in.close();
			}

		};

		protected void map(
				IntWritable key,
				VectorWritable value,
				org.apache.hadoop.mapreduce.Mapper<IntWritable, VectorWritable, IntWritable, VectorWritable>.Context context)
				throws IOException, InterruptedException {

			Vector original = value.get();
			Vector selected = new DenseVector(selectedIndices.size());
			int i = 0;
			for (int c : selectedIndices) {
				selected.set(i++, original.getQuick(c));
			}
			context.write(key, new VectorWritable(selected));
		};

	}

	private final static String COLS_PATH = "/tmp/rndcols";

	public void runRandomSelection(String originalMatrixPath, int numCols,
			int k, String outputMatrixPath, int numReducers)
			throws IOException, InterruptedException, ClassNotFoundException {

		List<Integer> l = new ArrayList<Integer>();
		for (int i = 0; i < numCols; i++)
			l.add(i);
		Collections.shuffle(l);

		// writing the random list to HDFS
		final Configuration conf = new Configuration();
		final FileSystem fs = FileSystem.get(conf);
		FSDataOutputStream out = fs.create(new Path(COLS_PATH));
		for (int i = 0; i < k; i++)
			out.writeInt(l.get(i));
		out.close();

		// launching the job
		conf.setStrings("selectedIndicesFile", COLS_PATH);

		Job job = new Job(conf);
		job.setJarByClass(RandomSelectionJob.class);
		FileInputFormat.addInputPaths(job, originalMatrixPath);
		FileOutputFormat.setOutputPath(job, new Path(outputMatrixPath));
		job.setMapperClass(RandomSelectionMapper.class);
		job.setReducerClass(Reducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(VectorWritable.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setNumReduceTasks(numReducers);
		job.waitForCompletion(false);
		if (!job.isSuccessful())
			throw new RuntimeException("RandomSelection Job is unsuccessful");
	}
}