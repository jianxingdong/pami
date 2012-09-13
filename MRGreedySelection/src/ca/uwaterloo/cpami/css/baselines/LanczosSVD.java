package ca.uwaterloo.cpami.css.baselines;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.hadoop.DistributedRowMatrix;
import org.apache.mahout.math.hadoop.decomposer.DistributedLanczosSolver;

public class LanczosSVD {

	/**
	 * Computes the left singular vectors matrix U (mxk). The matrix U will be
	 * stored in $outDirPath$/rawEigenvectors
	 * 
	 * @param args
	 *            [0] properties file path
	 * 
	 * 
	 * @throws Exception
	 */
	public static void _main(String[] args) throws Exception {

		Properties properties = new Properties();
		properties.load(FileSystem.get(new Configuration()).open(
				new Path(args[0])));

		Path APath = new Path(properties.getProperty("lanczos.input"));
		Path outDirPath = new Path(properties.getProperty("lanczos.outputDir"));
		Path tmpOutDirPath = new Path(
				properties.getProperty("lanczos.tmpOutDir"));
		Path tmpWorkDirPath = new Path(
				properties.getProperty("lanczos.tmpWorkDir"));

		int numRows = Integer.parseInt(properties
				.getProperty("lanczos.numRows"));
		int numCols = Integer.parseInt(properties
				.getProperty("lanczos.numCols"));

		DistributedRowMatrix A = new DistributedRowMatrix(APath,
				tmpWorkDirPath, numRows, numCols);
		Configuration conf = new Configuration();
		A.setConf(conf);
		DistributedRowMatrix AT = A.transpose();
		DistributedRowMatrix AAT = AT.times(AT);
		System.out.println("AAT Path: " + AAT.getRowPath());
		DistributedLanczosSolver lanczosSolver = new DistributedLanczosSolver();

		int success = lanczosSolver.run(AAT.getRowPath(), outDirPath,
				tmpOutDirPath, tmpWorkDirPath, numRows, numRows, true,
				Integer.parseInt(properties.getProperty("lanczos.k")), 0.5, 0,
				false);

		if (success != 0)
			throw new RuntimeException("Lanczos was unsuccessful");

		// printing the singular values
		Path eigVectrsPath = new Path("/lanc/Output", "cleanEigenvectors");

		final FileSystem fs = FileSystem.get(new Configuration());
		final SequenceFile.Reader reader = new SequenceFile.Reader(fs,
				eigVectrsPath, new Configuration());
		IntWritable key = new IntWritable();
		VectorWritable vw = new VectorWritable();
		while (reader.next(key, vw)) {
			NamedVector nv = (NamedVector) vw.get();
			String name = nv.getName();
			System.out.println(name);
			System.out.println(Math.sqrt(Double.parseDouble(name.substring(name
					.lastIndexOf('=') + 1))));
		}
		reader.close();
	}

	public static void main(String[] args) throws Exception {
		// DistributedRowMatrix A = new DistributedRowMatrix(new
		// Path("/lanc/B"),
		// new Path("/lanc/tmp/A/tmpdir"), 100, 100);
		// Configuration conf = new Configuration();
		// A.setConf(conf);

		// CSVToSequenceFile.csvToSequenceFile("B.csv", ",", 100, "/lanc/B");
		DistributedLanczosSolver lanczosSolver = new DistributedLanczosSolver();

		int success = lanczosSolver.run(new Path("/lanc/B"), new Path(
				"/lanc/tmp/out/t"), new Path("/lanc/tmp/outdir/t"), new Path(
				"/lanc/tmp/wrdkdir/t"), 100, 100, true, 10, 0, Float.MIN_VALUE,
				false);

		if (success != 0)
			throw new RuntimeException("Lanczos was unsuccessful");

		// printing the singular values
		Path eigVectrsPath = new Path("/lanc/tmp/out/t", "rawEigenvectors");

		final FileSystem fs = FileSystem.get(new Configuration());
		final SequenceFile.Reader reader = new SequenceFile.Reader(fs,
				eigVectrsPath, new Configuration());
		IntWritable key = new IntWritable();
		VectorWritable vw = new VectorWritable();
		while (reader.next(key, vw)) {
			NamedVector nv = (NamedVector) vw.get();
			String name = nv.getName();
			System.out.println(name);
			System.out.println(name.substring(name.lastIndexOf('=') + 1));
		}
		reader.close();

	}
}
