
import java.text.ParseException;
import java.text.SimpleDateFormat;

import java.util.Date;
import java.util.Random;

public class Testing {

	public static void main(String[] args) throws InterruptedException, ParseException {

		int i = 0;
		String types[] = { "ERROR", "INFO", "DEBUG" };
		String errors[] = { "Class not found exception", "Divide by Zero error", "Null Pointer error",
				"File not found error" };
		String warinings[] = { "Jar not added", "Warings for using methods", "multiple jars added as dependencies",
				"Memory is full for jvm" };
		String info[] = { "Sample info for spark", "job is started now", "job is ended now", "job is sucessfull" };
		SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date now = new Date();
		;
		System.out.println(String.format("%s%s", "/user/cloudera/",now.getTime()));
		Date logdate = sdfDate.parse("2017-07-28 01:19:36");
		long logTime = logdate.getTime();
		System.out.println(logTime);

		while (i < 100) {
			i++;
			 now = new Date();
			String timeStamp = sdfDate.format(now);
			int rnd = new Random().nextInt(types.length);
			int rnd1 = new Random().nextInt(errors.length);
			String data = "";
			if (rnd == 0) {
				data = timeStamp + "\t" + types[rnd] + "\t" + errors[rnd1];
			} else if (rnd == 1) {
				data = timeStamp + "\t" + types[rnd] + "\t" + info[rnd1];
			} else {
				data = timeStamp + "\t" + types[rnd] + "\t" + warinings[rnd1];
			}

			System.out.println(data);
			Thread.sleep(1000);
		}

	}

}
