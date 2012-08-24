package ca.uwaterloo.cpami.css.baselines;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HadoopUtils {
	public static Path getDataFilePath(String outputDirectory)
			throws IOException {
		FileSystem fs = FileSystem.get(new Configuration());
		FileStatus[] files = fs.listStatus(new Path(outputDirectory));
		for (FileStatus status : files) {
			Path p = status.getPath();
			if (!p.getName().startsWith("_"))
				return p;
		}
		return null;
	}
}
