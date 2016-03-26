package org.apress.prospark;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class SocketDriver extends AbstractDriver {

	private static final Logger LOG = LogManager.getLogger(SocketDriver.class);

	private String hostname;
	private int port;
	private SocketStream socketStream;

	public SocketDriver(String path, String hostname, int port) {
		super(path);
		this.hostname = hostname;
		this.port = port;
	}

	@Override
	public void init() throws Exception {
		socketStream = new SocketStream(hostname, port);
		LOG.info(String.format("Waiting for client to connect on port %d", port));
		SocketChannel socketChan = socketStream.init();
		LOG.info(String.format("Client %s connected on port %d", socketChan.getRemoteAddress(), port));
		socketStream.kickOff(socketChan);
		socketStream.start();
	}

	@Override
	public void close() throws IOException {
		socketStream.done();
		if (socketStream != null) {
			socketStream.close();
		}
	}

	@Override
	public void sendRecord(String record) throws Exception {
		socketStream.sendMsg(record + "\n");
	}

	static class SocketStream extends Thread {

		private String hostname;
		private int port;
		private ServerSocketChannel server;
		private volatile boolean isDone = false;
		private SocketChannel socket = null;
		private long totalBytes;
		private long totalLines;

		public SocketStream(String hostname, int port) {
			this.hostname = hostname;
			this.port = port;
			totalBytes = 0;
			totalLines = 0;
		}

		public SocketChannel init() throws IOException {
			server = ServerSocketChannel.open();
			server.bind(new InetSocketAddress(hostname, port));
			LOG.info(String.format("Listening on %s", server.getLocalAddress()));
			return server.accept();
		}

		public void kickOff(SocketChannel socket) {
			LOG.info("Kicking off data transfer");
			this.socket = socket;
		}

		@Override
		public void run() {
			try {
				while (!isDone) {
					Thread.sleep(1000);
				}
			} catch (Exception e) {
				LOG.error(e);
			}
		}

		public void sendMsg(String msg) throws IOException, InterruptedException, ExecutionException {
			if (socket != null) {
				ByteBuffer buffer = ByteBuffer.wrap(msg.getBytes(StandardCharsets.UTF_8));
				int bytesWritten = socket.write(buffer);
				totalBytes += bytesWritten;
			} else {
				throw new IOException("Client hasn't connected yet!");
			}
			totalLines++;
		}

		public void done() {
			isDone = true;
		}

		public void close() throws IOException {
			if (socket != null) {
				socket.close();
				socket = null;
			}
			LOG.info(String.format("SocketStream is closing after writing %d bytes and %d lines", totalBytes,
					totalLines));
		}
	}

	public static void main(String[] args) throws Exception {

		if (args.length != 3) {
			System.err.println("Usage: SocketDriver <path_to_input_folder> <hostname> <port>");
			System.exit(-1);
		}

		String path = args[0];
		String hostname = args[1];
		int port = Integer.parseInt(args[2]);

		SocketDriver driver = new SocketDriver(path, hostname, port);
		try {
			driver.execute();
		} finally {
			driver.close();
		}
	}
}