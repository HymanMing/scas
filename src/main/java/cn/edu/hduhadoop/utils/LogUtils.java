package cn.edu.hduhadoop.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Date Dec 18, 2014
 *
 * @Author GUI
 *
 * @Note Operate HDFS util class
 */
public class LogUtils {

	private static Logger LOGGER = LoggerFactory.getLogger(LogUtils.class);

	public static String[] line(String line) {
		String ip = ip(line);
		String timespan = timespan(line);
		String url = url(line);
		String hour = hour(line);

		return new String[] { ip, timespan, url, hour };
	}

	private static String ip(String line) {
		String ip = line.split(",")[7];
		return ip;
	}

	private static String timespan(String line) {
		String date = "";
		try {
			String dateStr = line.split(",")[3];
			date = StringUtil.unix2StringDate(dateStr, "yyyy-MM-dd HH:mm:ss");
		} catch (Exception ex) {
			ex.printStackTrace();
			LOGGER.error("Error line data is [" + line + "]");
		}
		return date;
	}

	private static String hour(String line) {
		String hour = "";
		try {
			String dateStr = line.split(",")[3];
			hour = StringUtil.unix2StringDate(dateStr, "HH");
		} catch (Exception ex) {
			ex.printStackTrace();
			LOGGER.error("Error line data is [" + line + "]");
		}
		return hour;
	}

	private static String url(String line) {
		String url = line.split(",")[4];
		return url;
	}

}
