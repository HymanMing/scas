package cn.edu.hduhadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import cn.edu.hduhadoop.utils.LogUtils;

/**
 * @Dec Dec 19, 2014
 *
 * @Author GUI
 *
 * @Note Use mapreduce task to clean hdfs data
 */
public class LogCleanMR {

	public static class LogMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {
			final String[] line = LogUtils.line(value.toString());

			// filter school info
			String[] url = line[2].split("cn/");
			if (url.length == 2) {
				line[2] = url[1];
			} else {
				line[2] = "";
			}

			// rebuild we need data
			String result = "";
			for (int i = 0; i < line.length; i++) {
				if (i == (line.length - 1)) {
					result += line[i];
				} else {
					result += line[i] + ",";
				}
			}
			context.write(key, new Text(result));
		}

	}

	public static class LogReducer extends Reducer<LongWritable, Text, Text, NullWritable> {

		@Override
		protected void reduce(LongWritable key, Iterable<Text> values,
				Reducer<LongWritable, Text, Text, NullWritable>.Context context) throws IOException,
				InterruptedException {
			for (Text v : values) {
				context.write(v, NullWritable.get());
			}
		}

	}

}
