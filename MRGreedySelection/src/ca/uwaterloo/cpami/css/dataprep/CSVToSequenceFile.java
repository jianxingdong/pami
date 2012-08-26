package ca.uwaterloo.cpami.css.dataprep;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.mahout.math.SequentialAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/**
 * docId wordId count -> matrix of words as instances and docs as features
 * colLength = numDocs
 * 
 * @see http://bickson.blogspot.ca/2011/02/mahout-svd-matrix-factorization.html
 */

public class CSVToSequenceFile {

	public static void csvToSequenceFile(String csvPath, String separator,
			int rowLength, String outputPath) {

		try {
			final Configuration conf = new Configuration();
			final FileSystem fs = FileSystem.get(conf);
			final SequenceFile.Writer writer = SequenceFile.createWriter(fs,
					conf, new Path(outputPath), IntWritable.class,
					VectorWritable.class, CompressionType.BLOCK);

			final IntWritable key = new IntWritable();
			final VectorWritable value = new VectorWritable();

			String thisLine;

			BufferedReader br = new BufferedReader(
					new InputStreamReader(FileSystem.get(new Configuration())
							.open(new Path(csvPath))));

			Vector vector = null;
			int from = -1, to = -1;
			int last_to = -1;
			float val = 0;
			int total = 0;
			int nnz = 0;
			int e = 0;
			int max_to = 0;
			int max_from = 0;

			while ((thisLine = br.readLine()) != null) { // while loop begins
															// here

				StringTokenizer st = new StringTokenizer(thisLine, separator);
				while (st.hasMoreTokens()) {

					to = Integer.parseInt(st.nextToken()) - 1; // convert from 1
					// based to zero
					// basd

					
					from = Integer.parseInt(st.nextToken()) - 1; // convert from
					// 1 based
					// to zero
					// based


					val = Float.parseFloat(st.nextToken());
					if (max_from < from)
						max_from = from;
					if (max_to < to)
						max_to = to;
					if (from < 0 || to < 0 || from > rowLength || val == 0.0)
						throw new NumberFormatException("wrong data" + from
								+ " to: " + to + " val: " + val);
				}

				// we are working on an existing column, set non-zero rows in it
				if (last_to != to && last_to != -1) {
					value.set(vector);

					writer.append(key, value); // write the older vector
					e += vector.getNumNondefaultElements();
				}
				// a new column is observed, open a new vector for it
				if (last_to != to) {
					vector = new SequentialAccessSparseVector(rowLength);
					key.set(to); // open a new vector
					total++;
				}

				vector.set(from, val);
				nnz++;

				if (nnz % 1000000 == 0) {
					System.out.println("Col" + total + " nnz: " + nnz);
				}
				last_to = to;

			} // end while

			value.set(vector);
			writer.append(key, value);// write last row
			e += vector.getNumNondefaultElements();
			total++;

			writer.close();
			System.out.println("Wrote a total of " + total + " cols "
					+ " nnz: " + nnz);
			if (e != nnz)
				System.err.println("Bug:missing edges! we only got" + e);

			System.out.println("Highest column: " + max_to + " highest row: "
					+ max_from);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

}
