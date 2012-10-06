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
import org.apache.mahout.math.VectorWritable;

public class FrobeniusNormJob {

	public static class FrobNormMapper extends
			Mapper<IntWritable, VectorWritable, NullWritable, DoubleWritable> {

		final DoubleWritable norm = new DoubleWritable();

		protected void map(
				IntWritable key,
				VectorWritable value,
				org.apache.hadoop.mapreduce.Mapper<IntWritable, VectorWritable, NullWritable, DoubleWritable>.Context context)
				throws java.io.IOException, InterruptedException {
			norm.set(Math.pow(value.get().norm(2), 2));
			context.write(NullWritable.get(), norm);
		};
	}

	public static class FrobNormReducer extends
			Reducer<NullWritable, DoubleWritable, NullWritable, DoubleWritable> {
		protected void reduce(
				NullWritable arg0,
				java.lang.Iterable<DoubleWritable> rowNorms,
				org.apache.hadoop.mapreduce.Reducer<NullWritable, DoubleWritable, NullWritable, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			Iterator<DoubleWritable> itr = rowNorms.iterator();
			double sum = 0;
			while (itr.hasNext()) {
				sum += itr.next().get();
			}
			context.write(NullWritable.get(), new DoubleWritable(sum));
		};
	}

	public double getFrobNorm(String matrixPath, String tmpDir)
			throws IOException, InterruptedException, ClassNotFoundException {

		final Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJarByClass(FrobeniusNormJob.class);
		FileInputFormat.addInputPaths(job, matrixPath);
		FileSystem fs = FileSystem.get(conf);
		Path outPath = new Path(tmpDir, "normfile");
		fs.delete(outPath, true);
		FileOutputFormat.setOutputPath(job, outPath);
		job.setMapperClass(FrobNormMapper.class);
		job.setReducerClass(FrobNormReducer.class);
		job.setCombinerClass(FrobNormReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setNumReduceTasks(1);
		job.waitForCompletion(false);
		if (!job.isSuccessful())
			throw new RuntimeException("FrobNorm Job is unsuccessful");

		FileStatus[] files = fs.listStatus(outPath, new PathFilter() {
			@Override
			public boolean accept(Path p) {
				return !p.getName().startsWith("_");
			}
		});

		final SequenceFile.Reader reader = new SequenceFile.Reader(fs,
				files[0].getPath(), conf);
		NullWritable key = NullWritable.get();
		DoubleWritable val = new DoubleWritable();
		reader.next(key, val);
		reader.close();
		return val.get();
	}

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		System.out.println(new FrobeniusNormJob().getFrobNorm("orth/nips.dat",
				"/tmpfrob"));
	}
}
