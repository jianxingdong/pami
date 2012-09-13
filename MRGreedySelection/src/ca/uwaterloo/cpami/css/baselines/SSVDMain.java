package ca.uwaterloo.cpami.css.baselines;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.hadoop.stochasticsvd.SSVDSolver;

public class SSVDMain {

	public static void main(String[] args) throws IOException,
			IllegalArgumentException, InterruptedException,
			ClassNotFoundException {

		// CSVToSequenceFile.csvToSequenceFile("/inv1/A.csv", ",", 100,
		// "/inv1/A");
		SSVDSolver ssvdSolver = new SSVDSolver(new Configuration(),
				new Path[] { new Path("/inv1/A") },
				new Path("/d/ssvd2/output"), 1000, 30, 2, 1);
		ssvdSolver.setQ(1);
		ssvdSolver.run();
		Vector singVals = ssvdSolver.getSingularValues();
		System.out.println("Top 10 Singular Values:");
		for (int i = 0; i < 10; i++) {
			System.out.print(singVals.get(i) + ", ");
		}
		System.out.println();
		/*
		 * DistributedRowMatrix A = new DistributedRowMatrix(new
		 * Path("/inv1/A"), new Path("/d/tmp/A"), 99, 100); A.setConf(new
		 * Configuration()); DistributedRowMatrix C = new
		 * DistributedRowMatrix(new Path( "/d/ssvd1/output/U/u-m-00000"), new
		 * Path("/d/tmp/C"), 99, 30); C.setConf(new Configuration());
		 * System.out.println(new ReconstructionError()
		 * .clacReconstructionErr(A, C,3));
		 */
	}
}
