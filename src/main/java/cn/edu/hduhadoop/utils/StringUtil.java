package cn.edu.hduhadoop.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * @Date Dec 18, 2014
 *
 * @Author GUI
 *
 * @Note String util class
 */
public class StringUtil {
	/**
	 * format "yyyy-MM-dd HH:mm:ss" like 1426439855 to 2014-12-16 14:17:35
	 * 
	 * <br/>
	 * <br/>
	 * 
	 * format "HH" like 1426439855 to 01
	 * 
	 * @param date
	 * @return
	 */
	public static String unix2StringDate(String date, String dateFormat)
			throws ParseException {
		SimpleDateFormat df = new SimpleDateFormat(dateFormat);
		return df.format(Long.parseLong(date) * 1000);
	}
}
