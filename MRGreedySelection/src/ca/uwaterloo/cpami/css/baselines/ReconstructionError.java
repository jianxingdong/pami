package ca.uwaterloo.cpami.css.baselines;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.hadoop.DistributedRowMatrix;

import ca.uwaterloo.cpami.css.dataprep.CSVToSequenceFile;
import ca.uwaterloo.cpami.mahout.matrix.utils.FrobeniusNormDiffJob;
import ca.uwaterloo.cpami.mahout.matrix.utils.PseudoInverse;

/**
 * 
 * Given two matrices A (the original matrix) and C (a matrix to project to its
 * span), the reconstruction error defined as ||A-C*(C'*C)^-1*C'*A||_F is
 * computed. The two matrices should be given as DistributedRowMatrix
 * 
 */
public class ReconstructionError {

	/**
	 * 
	 * @param A
	 * @param C
	 * @param numCColumns
	 *            : the leading numCColumns of will be only be considered
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 * 
	 */
	public double clacReconstructionErr(DistributedRowMatrix A,
			DistributedRowMatrix C) throws IllegalArgumentException,
			IOException, InterruptedException, ClassNotFoundException {
		PseudoInverse pseudoInverse = new PseudoInverse();
		// for now 1
		pseudoInverse.setNumReducers(1);
		DistributedRowMatrix cT = C.transpose();
		DistributedRowMatrix cTc = C.times(C);
		DistributedRowMatrix cTcInv = pseudoInverse.invert(
				new Path[] { cTc.getRowPath() }, cTc.numRows(), cTc.numCols());
		DistributedRowMatrix c_ctcInv = cT.times(cTcInv);
		DistributedRowMatrix c_ctcInv_t = c_ctcInv.transpose();
		DistributedRowMatrix cT_A = C.times(A);
		DistributedRowMatrix B = c_ctcInv_t.times(cT_A);

		return new FrobeniusNormDiffJob().calcFrobeniusNorm(A.getRowPath(),
				B.getRowPath());
	}

	// test
	public static void main(String[] args) throws IOException,
			IllegalArgumentException, InterruptedException,
			ClassNotFoundException {
		// CSVToSequenceFile.csvToSequenceFile("/inv/A.csv", ",", 100,
		// "/inv/A");
		CSVToSequenceFile.csvToSequenceFile("/inv/A.csv", ",", 100, "/inv/C");
		Configuration conf = new Configuration();
		DistributedRowMatrix A = new DistributedRowMatrix(new Path("/inv/A"),
				new Path("/tmpA/A"), 99, 100);
		A.setConf(conf);
		DistributedRowMatrix C = new DistributedRowMatrix(new Path("/inv/C"),
				new Path("/tmpC/C"), 99, 100);
		C.setConf(conf);
		System.out.println(new ReconstructionError()
				.clacReconstructionErr(A, C));

	}
}