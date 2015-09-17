package cn.edu.hduhadoop.main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.hduhadoop.mapreduce.LogCleanMR.LogMapper;
import cn.edu.hduhadoop.mapreduce.LogCleanMR.LogReducer;
import cn.edu.hduhadoop.utils.HDFSUtils;
import cn.edu.hduhadoop.utils.SystemConfig;

/**
 * @Date Dec 19, 2014
 * 
 * @author GUI
 * 
 * @Note HDFS task main function
 *
 */
public class LogCleanTask extends Configured implements Tool{

	private static Logger LOGGER = LoggerFactory.getLogger(LogCleanTask.class);
	private static Configuration conf;

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
			if (args.length != 1) {
				LOGGER.warn("args length must be 1 and as date param");
				return;
			}
			String tmpIn = SystemConfig.getProperty("hdfs.input.path.scas");
			String tmpOut = SystemConfig.getProperty("hdfs.output.path.scas");
			String inPath = String.format(tmpIn, "part-m-00000");
			String outPath = String.format(tmpOut, "cleaned/" + args[0]);

			// bak dfs file to old
			HDFSUtils.bak(tmpOut, outPath, "cleaned/" + args[0] + "-old", conf);

			args = new String[] { inPath, outPath };
			int res = ToolRunner.run(new Configuration(), new LogCleanTask(), args);
			System.exit(res);
		} catch (Exception ex) {
			ex.printStackTrace();
			LOGGER.error("Log clean task has error,msg is" + ex.getMessage());
		}

	}

	public int run(String[] args) throws Exception {
		final Job job = Job.getInstance(conf);
		job.setJarByClass(LogMapper.class);
		job.setMapperClass(LogMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(LogReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.setInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		int status = job.waitForCompletion(true) ? 0 : 1;
		return status;
	}

}
