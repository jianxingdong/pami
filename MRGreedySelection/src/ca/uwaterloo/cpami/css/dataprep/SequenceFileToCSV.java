package ca.uwaterloo.cpami.css.dataprep;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/**
 * 
 * @see http 
 *      ://bickson.blogspot.ca/2011/02/mahout-svd-matrix-factorization-reading
 *      .html
 * 
 */

public class SequenceFileToCSV {

	public static void sequenceFileToCSV(String sequenceFilePath,
			String csvPath, String separator) {

		try {
			final Configuration conf = new Configuration();
			final FileSystem fs = FileSystem.get(conf);
			FileStatus[] UParts = fs.listStatus(new Path(sequenceFilePath));
			BufferedWriter br = new BufferedWriter(new OutputStreamWriter(
					FileSystem.get(new Configuration()).create(
							new Path(csvPath))));
			IntWritable key = new IntWritable();
			VectorWritable vec = new VectorWritable();
			for (FileStatus fStat : UParts) {
				Path fPath = fStat.getPath();
				if (!fPath.getName().startsWith("_")) {
					final SequenceFile.Reader reader = new SequenceFile.Reader(
							fs, fPath, conf);

					while (reader.next(key, vec)) {
						// System.out.println("key " + key);
						Vector vect = vec.get();
						Iterator<Vector.Element> iter = vect.iterateNonZero();

						while (iter.hasNext()) {
							Vector.Element element = iter.next();
							br.write(key + separator + element.index()
									+ separator
									+ vect.getQuick(element.index()) + "\n");
						}
					}

					reader.close();
				}
			}
			br.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
}