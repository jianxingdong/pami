package ca.uwaterloo.cpami.css.baselines;

import java.io.IOException;
import java.util.Iterator;

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
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.SparseMatrix;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.function.DoubleFunction;

// via a single reducer
//should not orthogonalize at the master node
public class OrthogonalizationJob {

	public static class AggregateMapper extends
			Mapper<IntWritable, VectorWritable, IntWritable, VectorWritable> {

		final IntWritable zero = new IntWritable(0);
		final VectorWritable outVect = new VectorWritable();

		protected void map(
				IntWritable key,
				VectorWritable value,
				org.apache.hadoop.mapreduce.Mapper<IntWritable, VectorWritable, IntWritable, VectorWritable>.Context context)
				throws IOException, InterruptedException {
			outVect.set(new NamedVector(value.get(), key.get() + ""));
			context.write(zero, outVect);
		};
	}

	public static class OrthogonalizationReducer extends
			Reducer<IntWritable, VectorWritable, IntWritable, VectorWritable> {

		protected void reduce(
				IntWritable dummy,
				java.lang.Iterable<VectorWritable> rows,
				org.apache.hadoop.mapreduce.Reducer<IntWritable, VectorWritable, IntWritable, VectorWritable>.Context context)
				throws IOException, InterruptedException {
			// loads all of the rows in a single matrix
			Matrix mat = new SparseMatrix(context.getConfiguration().getInt(
					"numRows", -1), context.getConfiguration().getInt(
					"numCols", -1));
			Iterator<VectorWritable> rowsItr = rows.iterator();

			while (rowsItr.hasNext()) {
				NamedVector nv = (NamedVector) rowsItr.next().get();
				mat.assignRow(Integer.parseInt(nv.getName()), nv.getDelegate());
			}

			// apply gram-schmidt
			mat = mat.transpose();
			orthonormalize(mat, context);
			mat = mat.transpose();
			// write the matrix to the output
			int m = mat.numRows();
			IntWritable indexWritable = new IntWritable();
			VectorWritable rowWritable = new VectorWritable();
			for (int i = 0; i < m; i++) {
				indexWritable.set(i);
				rowWritable.set(mat.viewRow(i));
				context.write(indexWritable, rowWritable);
			}
		}

		// based on mahout GramSchmdit but here the rows are orthonormalized to
		// make used of Mahout's matrix sparse representation.
		private void orthonormalize(
				Matrix mx,
				org.apache.hadoop.mapreduce.Reducer<IntWritable, VectorWritable, IntWritable, VectorWritable>.Context context) {
			int n = mx.numRows();

			for (int c = 0; c < n; c++) {
				Vector col = mx.viewRow(c);
				for (int c1 = 0; c1 < c; c1++) {
					Vector viewC1 = mx.viewRow(c1);
					col.assign(col.minus(viewC1.times(viewC1.dot(col))));
				}
				final double norm2 = col.norm(2);
				if (norm2 == 0) {
					// it's a zero vector, have to discard it
					// TODO for now we leave it in the matrix since it will not
					// affect the rec-err
					System.out.println("zero col");
					continue;
				}
				col.assign(new DoubleFunction() {
					@Override
					public double apply(double x) {
						return x / norm2;
					}
				});
				context.progress();
			}

		}

	}

	public void runJob(String originalMatrixPath, int numRows, int numCols,
			String outputMatrixPath) throws IOException, InterruptedException,
			ClassNotFoundException {

		final Configuration conf = new Configuration();
		conf.setInt("numRows", numRows);
		conf.setInt("numCols", numCols);
		Job job = new Job(conf);
		job.setJarByClass(OrthogonalizationJob.class);
		FileInputFormat.addInputPaths(job, originalMatrixPath);
		FileOutputFormat.setOutputPath(job, new Path(outputMatrixPath));
		job.setMapperClass(AggregateMapper.class);
		job.setReducerClass(OrthogonalizationReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(VectorWritable.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setNumReduceTasks(1);
		job.waitForCompletion(false);
		if (!job.isSuccessful())
			throw new RuntimeException("Orthogonalization Job is unsuccessful");

	}

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {

		new OrthogonalizationJob().runJob("orth/nips.dat", 1500, 12419,
				"orth/nips.orth");
	}
}
