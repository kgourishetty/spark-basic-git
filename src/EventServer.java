import java.io.*; // wildcard import for brevity in tutorial
import java.net.*;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.*;

public class EventServer {
	private static final Executor SERVER_EXECUTOR = Executors.newSingleThreadExecutor();
	private static final int PORT = 6666;
	private static final String DELIMITER = ":";
	private static final long EVENT_PERIOD_SECONDS = 1;
	private static final Random random = new Random();

	public static void main(String[] args) throws IOException, InterruptedException {
		BlockingQueue<String> eventQueue = new ArrayBlockingQueue<>(100);
		SERVER_EXECUTOR.execute(new SteamingServer(eventQueue));
		while (true) {
			eventQueue.put(generateEvent());
			Thread.sleep(500);
		}
	}

	private static String generateEvent() {
		int i = 0;
		String types[] = { "ERROR", "INFO", "DEBUG" };
		String errors[] = { "Class not found exception", "Divide by Zero error", "Null Pointer error",
				"File not found error" };
		String warinings[] = { "Jar not added", "Warings for using methods", "multiple jars added as dependencies",
				"Memory is full for jvm" };
		String info[] = { "Sample info for spark", "job is started now", "job is ended now", "job is sucessfull" };
		SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date now = new Date();
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
		return data;
	}

	private static class SteamingServer implements Runnable {
		private final BlockingQueue<String> eventQueue;

		public SteamingServer(BlockingQueue<String> eventQueue) {
			this.eventQueue = eventQueue;
		}

		@Override
		public void run() {
			try (ServerSocket serverSocket = new ServerSocket(PORT);
					Socket clientSocket = serverSocket.accept();
					PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);) {
				while (true) {
					String event = eventQueue.take();
					System.out.println(String.format("Writing \"%s\" to the socket.", event));
					out.println(event);
				}
			} catch (IOException | InterruptedException e) {
				throw new RuntimeException("Server error", e);
			}
		}
	}
}