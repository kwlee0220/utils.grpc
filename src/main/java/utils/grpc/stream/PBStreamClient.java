package utils.grpc.stream;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import utils.grpc.stream.client.PBStreamServiceProxy;
import utils.grpc.stream.client.StreamUploadOutputStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PBStreamClient {
	private final Server m_server;
	
	private final ManagedChannel m_channel;
	private final ExecutorService m_executor;
	private final PBStreamServiceProxy m_service;
	
	public static PBStreamClient connect(String host, int port) throws IOException {
		ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
													.usePlaintext()
													.build();
		
		return new PBStreamClient(channel);
	}
	
	private PBStreamClient(ManagedChannel channel) throws IOException {
		m_channel = channel;
		
		m_executor = Executors.newFixedThreadPool(16);
		m_service = new PBStreamServiceProxy(channel);

		m_server = ServerBuilder.forPort(0).build();
		m_server.start();
	}
	
	public Server getGrpcServer() {
		return m_server;
	}
	
	public void disconnect() {
		m_channel.shutdown();
		m_server.shutdown();
		m_executor.shutdown();
	}
	
	ManagedChannel getChannel() {
		return m_channel;
	}
	
	public PBStreamServiceProxy getStreamService() {
		return m_service;
	}

	public InputStream download(String path) throws IOException {
		return m_service.download(path);
	}

	public long upload(String path, InputStream stream) throws IOException {
		return m_service.upload(path, stream);
	}

	public StreamUploadOutputStream openUploadOutputStream(String path) {
		return m_service.openUploadOutputStream(path);
	}
}
