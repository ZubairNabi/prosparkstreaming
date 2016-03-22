package org.apress.prospark;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class SocketDriver extends AbstractDriver {

	private static final Logger LOG = LogManager.getLogger(SocketDriver.class);

	private int port;
	private SocketStream socketStream;

	public SocketDriver(String path, int port) {
		super(path);
		this.port = port;
	}

	@Override
	public void init() throws Exception {
		socketStream = new SocketStream(port);
		Future<AsynchronousSocketChannel> socketFuture = socketStream.init();
		LOG.info(String.format("Waiting for client to connect on port %d", port));
		AsynchronousSocketChannel socket = socketFuture.get();
		LOG.info(String.format("Client %s connected on port %d", socket.getRemoteAddress(), port));
		socketStream.kickOff(socket);
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

		private int port;
		private AsynchronousServerSocketChannel server;
		private Future<AsynchronousSocketChannel> socketFuture = null;
		private volatile boolean isDone = false;
		private AsynchronousSocketChannel socket = null;
		private long totalBytes;
		private long totalLines;

		public SocketStream(int port) {
			this.port = port;
			totalBytes = 0;
			totalLines = 0;
		}

		public Future<AsynchronousSocketChannel> init() throws IOException {
			server = AsynchronousServerSocketChannel.open();
			server.bind(new InetSocketAddress("localhost", port));
			LOG.info(String.format("Listening on %s", server.getLocalAddress()));
			socketFuture = server.accept();
			return socketFuture;
		}

		public void kickOff(AsynchronousSocketChannel socket) {
			LOG.info("Kicking off data transfer");
			this.socket = socket;
		}

		@Override
		public void run() {
			try {
				while (!isDone)
					;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		public void sendMsg(String msg) throws IOException, InterruptedException, ExecutionException {
			if (socket != null) {
				ByteBuffer buffer = ByteBuffer.wrap(msg.getBytes(StandardCharsets.UTF_8));
				Future<Integer> bytesWrittenFuture = socket.write(buffer);
				totalBytes += bytesWrittenFuture.get();
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

		if (args.length != 2) {
			System.err.println("Usage: SocketDriver <path_to_input_folder> <port>");
			System.exit(-1);
		}

		String path = args[0];
		int port = Integer.parseInt(args[1]);

		SocketDriver driver = new SocketDriver(path, port);
		try {
			driver.execute();
		} finally {
			driver.close();
		}
	}
}