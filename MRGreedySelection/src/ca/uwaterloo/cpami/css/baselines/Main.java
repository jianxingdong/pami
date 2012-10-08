package ca.uwaterloo.cpami.css.baselines;

import java.io.IOException;
import java.io.PrintWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.hadoop.DistributedRowMatrix;
import org.apache.mahout.math.hadoop.stochasticsvd.SSVDSolver;

import ca.uwaterloo.cpami.css.greedy.mapreduce.rowpersample.GCSSDriver;
import ca.uwaterloo.cpami.mahout.matrix.utils.Helpers;

public class Main {

	// cluster size = 20 nodes
	static String origDataFile = "/common/A";

	static int[] ks = { 1000 };// , 500, 1000, 1500, 2000

	static int numCols = 200000;// 8200000;

	static int numRows = 141043;

	static FileSystem fs;
	static String tmpMatA = "/tmp/Atmp/tA";

	static int numReducers = 9;

	static Configuration config = new Configuration();
	static {

		try {
			fs = FileSystem.get(config);
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	static void runGCSS(int k, int numPartitions, float l) throws IOException,
			InterruptedException, ClassNotFoundException {
		DistributedRowMatrix A = new DistributedRowMatrix(
				new Path(origDataFile), new Path(tmpMatA), numRows, numCols);
		A.setConf(config);
		PrintWriter pw = new PrintWriter(fs.create(
				new Path("/results/CSS.txt"), true));

		String tmpCSS = "/tmp/css/tcc";
		String tmpMatC = "/tmp/Ctmp/tc";

		String cssNewMatrix = "greedycss/C";
		String cssNewMatrixOrth = "greedycss/COrth";
		String tmpNormDir = "/tmp/tmpNorm";
		GCSSDriver gcssDriver = new GCSSDriver();
		// for (int k : ks) {
		System.out.println(k);
		fs.delete(new Path(tmpCSS), true);
		fs.delete(new Path(tmpMatC), true);
		fs.delete(new Path(cssNewMatrix), true);
		fs.delete(new Path(cssNewMatrixOrth), true);
		fs.delete(new Path(tmpNormDir), true);
		long time = System.currentTimeMillis();
		boolean succuss = gcssDriver.run(A.getRowPath().toString(), tmpCSS,
				numPartitions, cssNewMatrix, numRows, numCols, k, l,
				numReducers);
		if (!succuss) {
			throw new RuntimeException("GCSS Failed");
		}
		long duration = System.currentTimeMillis() - time;
		// Orthogonalization
		System.out.println("start orth");
		time = System.currentTimeMillis();
		new OrthogonalizationJob().runJob(cssNewMatrix, numRows, k,
				cssNewMatrixOrth);
		System.out.println("orth done: " + (System.currentTimeMillis() - time));
		double recErr = MultiplicationNormJob.calcMultiplicationNorm(
				A.getRowPath(), new Path(cssNewMatrixOrth),
				new Path(tmpNormDir), numCols, numReducers);
		System.out.println("all done, err duration: "
				+ (System.currentTimeMillis() - time));
		pw.println(k + "\t" + duration + "\t" + recErr);
		System.out.println(k + "\t" + duration + "\t" + recErr);
		pw.flush();
		// }
		pw.close();
	}

	static void runRandomSelection(int k) throws IOException,
			InterruptedException, ClassNotFoundException {
		DistributedRowMatrix A = new DistributedRowMatrix(
				new Path(origDataFile), new Path(tmpMatA), numRows, numCols);
		A.setConf(config);
		PrintWriter pw = new PrintWriter(fs.create(new Path(
				"/results/Random.txt"), true));

		String tmpMatC = "/tmp/Ctmp/CT";
		String tmpNormDir = "/tmp/tmpNorm";
		String randomNewMatrix = "random/Rnd/R";
		String randomNewMatrixOrth = "random/Rnd/ROrth";
		RandomSelectionJob randomSelectionJob = new RandomSelectionJob();
		// for (int k : ks) {
		System.out.println(k);
		fs.delete(new Path(tmpMatC), true);
		fs.delete(new Path(randomNewMatrix), true);
		fs.delete(new Path(tmpNormDir), true);
		long time = System.currentTimeMillis();
		randomSelectionJob.runRandomSelection(A.getRowPath().toString(),
				numCols, k, randomNewMatrix);
		long duration = System.currentTimeMillis() - time;
		System.out.println("start done");
		time = System.currentTimeMillis();
		new OrthogonalizationJob().runJob(randomNewMatrix, numRows, k,
				randomNewMatrixOrth);
		System.out.println("orth done: " + (System.currentTimeMillis() - time));
		double recErr = MultiplicationNormJob.calcMultiplicationNorm(A
				.getRowPath(), new Path(randomNewMatrixOrth), new Path(
				tmpNormDir), numCols, numReducers);
		System.out.println("all done, err duration: "
				+ (System.currentTimeMillis() - time));
		pw.println(k + "\t" + duration + "\t" + recErr);
		System.out.println(k + "\t" + duration + "\t" + recErr);
		pw.flush();
		// }
		pw.close();
	}

	static void runSSVD(int k, int p, int aBlockRows) throws IOException,
			IllegalArgumentException, InterruptedException,
			ClassNotFoundException {
		DistributedRowMatrix A = new DistributedRowMatrix(
				new Path(origDataFile), new Path(tmpMatA), numRows, numCols);
		A.setConf(config);
		String ssvdOutput = "ssvd/output";
		String tmpNormDir = "/tmp/tmpNorm";

		PrintWriter pw = new PrintWriter(fs.create(
				new Path("/results/SSVD.txt"), true));

		String tmpMatC = "/tmp/Ctmp";
		// for (int k : ks) {
		fs.delete(new Path(tmpMatC), true);
		fs.delete(new Path(ssvdOutput), true);
		fs.delete(new Path(tmpNormDir), true);

		SSVDSolver ssvdSolver = new SSVDSolver(config,
				new Path[] { A.getRowPath() }, new Path(ssvdOutput),
				aBlockRows, k, p, numReducers);
		ssvdSolver.setQ(1);
		ssvdSolver.setComputeV(false);
		long time = System.currentTimeMillis();
		ssvdSolver.run();
		long duration = System.currentTimeMillis() - time;
		/*
		 * DistributedRowMatrix C = new DistributedRowMatrix(new Path(
		 * ssvdSolver.getUPath()), new Path(tmpMatC), numRows, k);
		 * C.setConf(config);
		 * 
		 * double recErr = new ReconstructionError().clacReconstructionErr(A, C,
		 * Main.numReducers);
		 * 
		 * pw.println(k + "\t" + duration + "\t" + recErr);
		 */
		time = System.currentTimeMillis();

		double recErr = MultiplicationNormJob.calcMultiplicationNorm(A
				.getRowPath(), new Path(ssvdSolver.getUPath()), new Path(
				tmpNormDir), numCols, numReducers);
		System.out.println("all done, err duration: "
				+ (System.currentTimeMillis() - time));
		pw.println(k + "\t" + duration + "\t" + recErr);
		System.out.println(k + "\t" + duration + "\t" + recErr);
		pw.flush();
		// }
		pw.close();
	}

	static void repartitionA() throws IOException, InterruptedException,
			ClassNotFoundException {		
		Helpers.repartitionMatrix(new Path("/common/B"), new Path("/common/A"),
				1);		
	}

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {
		/*
		 * DistributedRowMatrix A = new DistributedRowMatrix( new
		 * Path("s3n://greedycss/pubmed-doc-word.dat"), new Path(tmpMatA),
		 * 8200000, 141043); A.setConf(config); A = A.transpose();
		 * 
		 * new
		 * FirstKColumnsSelectionJob().selectFirstKCols(A.getRowPath().toString
		 * (), 200000, "/common/A");
		 */
		// runRandomSelection(Integer.parseInt(args[0]));
		if (args.length > 3)
			repartitionA();

		runGCSS(Integer.parseInt(args[0]), Integer.parseInt(args[1]),
				Float.parseFloat(args[2]));
		/*
		 * runSSVD(Integer.parseInt(args[0]), Integer.parseInt(args[1]),
		 * Integer.parseInt(args[2]));
		 */
		// System.out.println(">>>>>>>>>Random Selection-DONE");
		// runGCSS();
		// System.out.println(">>>>>>>>>GCSS-DONE");
		// runSSVD();
		// System.out.println(">>>>>>>>>SSVD-DONE");
	}
}