package ca.uwaterloo.cpami.mahout.matrix.utils;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/**
 * 
 * Given Two Matrices A and B, the job computes the Frobenius Norm of A-B
 * 
 */
public class FrobeniusNormDiffJob {

	public static class FrobNormReducer extends
			Reducer<IntWritable, VectorWritable, NullWritable, DoubleWritable> {

		protected void reduce(
				IntWritable rowId,
				java.lang.Iterable<VectorWritable> twoRows,
				org.apache.hadoop.mapreduce.Reducer<IntWritable, VectorWritable, NullWritable, DoubleWritable>.Context context)
				throws java.io.IOException, InterruptedException {

			Iterator<VectorWritable> itr = twoRows.iterator();
			Vector v1 = itr.next().get();
			Vector v2 = itr.next().get();
			context.write(NullWritable.get(),
					new DoubleWritable(Math.pow(v1.minus(v2).norm(2), 2)));
		};

	}

	private double sumValues(Path seqFilePath, Configuration conf)
			throws IOException {
		final FileSystem fs = FileSystem.get(conf);
		FileStatus[] files = fs.listStatus(seqFilePath, new PathFilter() {

			@Override
			public boolean accept(Path p) {
				return !p.getName().startsWith("_");
			}
		});
		double sum = 0;
		for (FileStatus f : files) {
			final SequenceFile.Reader reader = new SequenceFile.Reader(fs,
					f.getPath(), conf);
			NullWritable key = NullWritable.get();
			DoubleWritable val = new DoubleWritable();

			while (reader.next(key, val)) {
				sum += val.get();
			}
			reader.close();
		}
		return sum;
	}

	private final static String ROW_SUM_PATH = "/tmp/rowFrobNorm";

	public double calcFrobeniusNorm(Path A, Path B, int numReducers)
			throws IOException, InterruptedException, ClassNotFoundException {

		final Configuration conf = new Configuration();
		FileSystem.get(conf).delete(new Path(ROW_SUM_PATH), true);
		Job job = new Job(conf);
		job.setJarByClass(FrobeniusNormDiffJob.class);
		FileInputFormat.addInputPaths(job, A.toString());
		FileInputFormat.addInputPaths(job, B.toString());
		FileOutputFormat.setOutputPath(job, new Path(ROW_SUM_PATH));
		job.setMapperClass(Mapper.class);
		job.setReducerClass(FrobNormReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(VectorWritable.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setNumReduceTasks(numReducers);
		job.waitForCompletion(false);
		if (job.isSuccessful())
			return Math.sqrt(sumValues(new Path(ROW_SUM_PATH), conf));
		throw new RuntimeException("Frobenius Norm Job is unsuccessful");
	}

}
