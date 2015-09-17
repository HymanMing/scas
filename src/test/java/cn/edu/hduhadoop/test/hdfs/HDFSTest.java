package cn.edu.hduhadoop.test.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.hduhadoop.utils.SystemConfig;

/**
 * @Date Dec 17, 2014
 *
 * @Author GUI
 *
 * @Note Test hadoop cluster with hdfs ha
 */
public class HDFSTest {

	private static Configuration conf = null;
	private static Logger LOGGER = LoggerFactory.getLogger(HDFSTest.class);

	/**
	 * Set ha cluster info
	 */
	static {
		String tag = SystemConfig.getProperty("dev.tag");
		String[] hosts = SystemConfig.getPropertyArray(tag + ".hdfs.host", ",");
		conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://cluster1");
		conf.set("dfs.nameservices", "cluster1");
		conf.set("dfs.ha.namenodes.cluster1", "nna,nns");
		conf.set("dfs.namenode.rpc-address.cluster1.nna", hosts[0]);
		conf.set("dfs.namenode.rpc-address.cluster1.nns", hosts[1]);
		conf.set("dfs.client.failover.proxy.provider.cluster1",
				"org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
	}

	public static void main(String[] args) {
		try {
			dfs();
		} catch (Exception e) {
			e.printStackTrace();
			LOGGER.error("Run dfs has error,msg is " + e.getMessage());
		}
	}

	private static void dfs() {
		FileSystem fs = null;
		try {
			fs = FileSystem.get(conf);// get file object
			FileStatus[] list = fs.listStatus(new Path("/"));// file status list
			for (FileStatus file : list) {
				LOGGER.info(file.getPath().getName());// print file names
			}
		} catch (IOException e) {
			e.printStackTrace();
			LOGGER.error("Get hdfs path has error,msg is " + e.getMessage());
		} finally {
			try {
				if (fs != null) {
					fs.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
				LOGGER.error("Close fs object has error,msg is " + e.getMessage());
			}
		}
	}

}
