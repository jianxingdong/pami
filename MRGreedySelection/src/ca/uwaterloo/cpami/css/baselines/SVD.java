package ca.uwaterloo.cpami.css.baselines;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.hadoop.stochasticsvd.SSVDSolver;

import ca.uwaterloo.cpami.css.dataprep.CSVToSequenceFile;
import ca.uwaterloo.cpami.css.dataprep.SequenceFileToCSV;

public class SVD {

	/**
	 * args[0] parameters file path
	 * 
	 * @throws IOException
	 * @throws FileNotFoundException
	 * 
	 */
	public static void main(String[] args) throws FileNotFoundException,
			IOException {

		Properties properties = new Properties();
		
		properties.load(FileSystem.get(new Configuration()).open(
				new Path(args[0])));

		CSVToSequenceFile.csvToSequenceFile(properties.getProperty("csvPath"),
				properties.getProperty("csvSeparator"),
				Integer.parseInt(properties.getProperty("colLength")),
				properties.getProperty("sequenceFilePath"));

		// applying stochastic SVD
		Configuration conf = new Configuration();
		SSVDSolver ssvdSolver = new SSVDSolver(conf, new Path[] { new Path(
				properties.getProperty("sequenceFilePath")) }, new Path(
				properties.getProperty("ssvd.ouputPath")),
				Integer.parseInt(properties.getProperty("ssvd.aBlockRows")),
				Integer.parseInt(properties.getProperty("ssvd.k")),
				Integer.parseInt(properties.getProperty("ssvd.p")),
				Integer.parseInt(properties.getProperty("ssvd.reduceTasks")));

		ssvdSolver.run();
		String uMatrixPath = ssvdSolver.getUPath();
		SequenceFileToCSV.sequenceFileToCSV(uMatrixPath,
				properties.getProperty("ssvd.UMatrixPath"),
				properties.getProperty("ssvd.UCSVSeparator"));

		// printing the top 10 singular values

		Vector singVals = ssvdSolver.getSingularValues();
		System.out.println("Top 10 Singular Values:");
		for (int i = 0; i < 10; i++) {
			System.out.print(singVals.get(i) + ", ");
		}
		System.out.println();
	}

	public static void _main(String[] args) throws FileNotFoundException,
			IOException {
		Properties p = new Properties();
		p.load(new FileReader("resources/properties.txt"));
		StringTokenizer st = new StringTokenizer("1 2 3", p.getProperty("csvSeparator"));
		while(st.hasMoreTokens())
			System.out.println(st.nextToken());

	}
}
