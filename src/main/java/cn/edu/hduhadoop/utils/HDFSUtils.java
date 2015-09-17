package cn.edu.hduhadoop.utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSUtils {

	/**
	 * bak dfs file to old file
	 * 
	 * @param tmpOut
	 * @param outPath
	 * @param metaName
	 * @param conf
	 * @throws IOException
	 */
	public static void bak(String tmpOut, String outPath, String metaName, Configuration conf) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path dst = new Path(outPath);
		if (fs.exists(dst)) {
			String oldPath = String.format(tmpOut, metaName);
			if (fs.exists(new Path(oldPath))) {
				fs.delete(new Path(oldPath), true);
				fs.rename(new Path(outPath), new Path(oldPath));
			} else {
				fs.rename(new Path(outPath), new Path(oldPath));
			}
		}
	}
}
