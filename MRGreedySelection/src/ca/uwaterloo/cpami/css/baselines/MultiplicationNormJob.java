package ca.uwaterloo.cpami.css.baselines;

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
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.join.CompositeInputFormat;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.function.Functions;

import ca.uwaterloo.cpami.mahout.matrix.utils.Helpers;

//given two matrices Q and A, the job compute ||Q'A|| where ||.|| is the frob. norm
public class MultiplicationNormJob {

	public static double calcMultiplicationNorm(Path A, Path Q, Path tmpDir,
			int n, int numReducers) throws IOException, InterruptedException,
			ClassNotFoundException {
		// fix Q partitioning if needed
		int CNumParts = Helpers.getNumPrtitions(Q);
		int ANumParts = Helpers.getNumPrtitions(A);
		if (CNumParts != ANumParts) {
			Path newQPath = new Path(tmpDir, Q.getName() + "-repartition");
			Helpers.repartitionMatrix(Q, newQPath, ANumParts);
			Q = newQPath;
		}
		// calc the norm of each row
		Path tmpNormsFile = new Path(tmpDir, "tmpNorms");

		JobClient.runJob(new JobConf(createMatrixMultiplyJobConf(
				new Configuration(), Q, A, tmpNormsFile, n, numReducers)));
		// sum row norms
		return sumValues(tmpNormsFile, new Configuration());
	}

	private static double sumValues(Path seqFilePath, Configuration conf)
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

	private static final String OUT_CARD = "output.vector.cardinality";

	public static Configuration createMatrixMultiplyJobConf(
			Configuration initialConf, Path aPath, Path bPath, Path outPath,
			int outCardinality, int numReducers) {
		JobConf conf = new JobConf(initialConf, MultiplicationNormJob.class);
		conf.setInputFormat(CompositeInputFormat.class);
		conf.set("mapred.join.expr", CompositeInputFormat.compose("inner",
				SequenceFileInputFormat.class, aPath, bPath));
		conf.setInt(OUT_CARD, outCardinality);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		FileOutputFormat.setOutputPath(conf, outPath);
		conf.setMapperClass(MatrixMultiplyMapper.class);
		// conf.setCombinerClass(MatrixMultiplicationReducer.class);
		conf.setReducerClass(MatrixMultiplicationReducer.class);

		conf.setOutputKeyClass(NullWritable.class);
		conf.setOutputValueClass(DoubleWritable.class);
		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(VectorWritable.class);

		conf.setNumReduceTasks(numReducers);
		return conf;
	}

	public static class MatrixMultiplyMapper extends MapReduceBase implements
			Mapper<IntWritable, TupleWritable, IntWritable, VectorWritable> {

		private int outCardinality;
		private final IntWritable row = new IntWritable();

		@Override
		public void configure(JobConf conf) {
			outCardinality = conf.getInt(OUT_CARD, Integer.MAX_VALUE);
		}

		@Override
		public void map(IntWritable index, TupleWritable v,
				OutputCollector<IntWritable, VectorWritable> out,
				Reporter reporter) throws IOException {
			boolean firstIsOutFrag = ((VectorWritable) v.get(0)).get().size() == outCardinality;
			Vector outFrag = firstIsOutFrag ? ((VectorWritable) v.get(0)).get()
					: ((VectorWritable) v.get(1)).get();
			Vector multiplier = firstIsOutFrag ? ((VectorWritable) v.get(1))
					.get() : ((VectorWritable) v.get(0)).get();

			VectorWritable outVector = new VectorWritable();
			Iterator<Vector.Element> it = multiplier.iterateNonZero();
			while (it.hasNext()) {
				Vector.Element e = it.next();
				row.set(e.index());
				outVector.set(outFrag.times(e.get()));
				out.collect(row, outVector);
			}
		}
	}

	public static class MatrixMultiplicationReducer extends MapReduceBase
			implements
			Reducer<IntWritable, VectorWritable, NullWritable, DoubleWritable> {

		@Override
		public void reduce(IntWritable rowNum, Iterator<VectorWritable> it,
				OutputCollector<NullWritable, DoubleWritable> out,
				Reporter reporter) throws IOException {
			if (!it.hasNext()) {
				return;
			}
			Vector accumulator = new RandomAccessSparseVector(it.next().get());
			while (it.hasNext()) {
				Vector row = it.next().get();
				accumulator.assign(row, Functions.PLUS);
			}

			out.collect(NullWritable.get(),
					new DoubleWritable(Math.pow(accumulator.norm(2), 2)));
		}
	}

}
