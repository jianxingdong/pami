package ca.uwaterloo.cpami.css.baselines;

import java.io.IOException;
import java.io.PrintWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.hadoop.DistributedRowMatrix;
import org.apache.mahout.math.hadoop.stochasticsvd.SSVDSolver;

import ca.uwaterloo.cpami.css.greedy.mapreduce.rowpersample.Driver;
import ca.uwaterloo.cpami.mahout.matrix.utils.CentralizedOrthogonalization;

public class Main {

	// cluster size = 20 nodes
	static String origDataFile = "/common/A";

	static int[] ks = { 100 };// , 500, 1000, 1500, 2000

	static int numCols = 8200000;

	static int numRows = 141043;

	static FileSystem fs;
	static String tmpMatA = "/tmp/Atmp/tA";

	static DistributedRowMatrix A = new DistributedRowMatrix(new Path(
			origDataFile), new Path(tmpMatA), numCols, numRows);

	static int numReducers = 80;

	static Configuration config = new Configuration();
	static {
		try {

			fs = FileSystem.get(config);
			A.setConf(config);
			A = A.transpose();
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	static void runGCSS() throws IOException, InterruptedException,
			ClassNotFoundException {

		PrintWriter pw = new PrintWriter(fs.create(
				new Path("/results/CSS.txt"), true));

		String tmpCSS = "/tmp/css/tcc";
		String tmpMatC = "/tmp/Ctmp/tc";

		int numPartitions = 800;
		String cssNewMatrix = "greedycss/C";
		String cssNewMatrixOrth = "greedycss/COrth";
		float l = 0.2f;

		for (int k : ks) {
			System.out.println(k);
			fs.delete(new Path(tmpCSS), true);
			fs.delete(new Path(tmpMatC), true);
			fs.delete(new Path(cssNewMatrix), true);
			long time = System.currentTimeMillis();
			Driver.main(new String[] { A.getRowPath().toString(), tmpCSS,
					numPartitions + "", cssNewMatrix, numCols + "", k + "",
					l + "" });
			long duration = System.currentTimeMillis() - time;
			// Orthogonalization
			CentralizedOrthogonalization.orthonormalize(new Path(cssNewMatrix),
					numRows, k, new Path(cssNewMatrixOrth));
			DistributedRowMatrix C = new DistributedRowMatrix(new Path(
					cssNewMatrixOrth), new Path(tmpMatC), numRows, k);
			C.setConf(config);

			double recErr = new ReconstructionError().clacReconstructionErr(A,
					C, numReducers);
			pw.println(k + "\t" + duration + "\t" + recErr);
			pw.flush();
		}
		pw.close();
	}

	static void runRandomSelection() throws IOException, InterruptedException,
			ClassNotFoundException {
		PrintWriter pw = new PrintWriter(fs.create(new Path(
				"/results/Random.txt"), true));

		String tmpMatC = "/tmp/Ctmp/CT";
		String tmpNormDir = "/tmp/tmpNorm";
		String randomNewMatrix = "random/Rnd/R";
		String randomNewMatrixOrth = "random/Rnd/ROrth";
		RandomSelectionJob randomSelectionJob = new RandomSelectionJob();
		for (int k : ks) {
			System.out.println(k);
			fs.delete(new Path(tmpMatC), true);
			fs.delete(new Path(randomNewMatrix), true);
			fs.delete(new Path(tmpNormDir), true);
			long time = System.currentTimeMillis();
			randomSelectionJob.runRandomSelection(A.getRowPath().toString(),
					numCols, k, randomNewMatrix, numReducers);
			long duration = System.currentTimeMillis() - time;
			System.out.println("start done");
			time = System.currentTimeMillis();
			new OrthogonalizationJob().runJob(randomNewMatrix, numRows, k,
					randomNewMatrixOrth);
			System.out.println("orth done");
			double recErr = MultiplicationNormJob.calcMultiplicationNorm(A
					.getRowPath(), new Path(randomNewMatrixOrth), new Path(
					tmpNormDir), numCols, numReducers);
			System.out.println("all done, err duration: "+(System.currentTimeMillis() - time));
			pw.println(k + "\t" + duration + "\t" + recErr);
			pw.flush();
		}
		pw.close();
	}

	static void runSSVD() throws IOException, IllegalArgumentException,
			InterruptedException, ClassNotFoundException {

		String ssvdOutput = "ssvd/output";
		int aBlockRows = 10000;
		PrintWriter pw = new PrintWriter(fs.create(
				new Path("/results/SSVD.txt"), true));

		String tmpMatC = "/tmp/Ctmp";
		for (int k : ks) {
			fs.delete(new Path(tmpMatC), true);
			fs.delete(new Path(ssvdOutput), true);
			SSVDSolver ssvdSolver = new SSVDSolver(config,
					new Path[] { A.getRowPath() }, new Path(ssvdOutput),
					aBlockRows, k, (int) (0.1 * k), numReducers);
			ssvdSolver.setQ(1);
			long time = System.currentTimeMillis();
			ssvdSolver.run();
			long duration = System.currentTimeMillis() - time;
			DistributedRowMatrix C = new DistributedRowMatrix(new Path(
					ssvdSolver.getUPath()), new Path(tmpMatC), numRows, k);
			C.setConf(config);
			double recErr = new ReconstructionError().clacReconstructionErr(A,
					C, Main.numReducers);
			pw.println(k + "\t" + duration + "\t" + recErr);
			pw.flush();
		}
		pw.close();
	}

	public static void main(String[] args) throws IOException,
			InterruptedException, ClassNotFoundException {

		runRandomSelection();
		System.out.println(">>>>>>>>>Random Selection-DONE");
		// runGCSS();
		// System.out.println(">>>>>>>>>GCSS-DONE");
		// runSSVD();
		// System.out.println(">>>>>>>>>SSVD-DONE");
	}
}