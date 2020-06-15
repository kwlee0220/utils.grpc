package utils.grpc.stream;

import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.logging.LogManager;

import org.apache.log4j.PropertyConfigurator;

import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.codec.http2.Http2Exception.StreamException;
import utils.NetUtils;
import utils.grpc.stream.server.PBStreamServiceServant;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PBStreamServerMain {
	private static final int DEFAULT_PORT = 15685;
	
	private int m_port = -1;
	
	private PBStreamServerMain(int port) {
		m_port = port;
	}
	
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
//		LogManager.getRootLogger().setLevel(Level.OFF);
		
		LogManager.getLogManager().reset();
		
		int port = ( args.length > 0 ) ? Integer.parseInt(args[0]) : DEFAULT_PORT;
		PBStreamServerMain main = new PBStreamServerMain(port);
		main.start();
	}
	
	private void start() throws IOException, InterruptedException {
		PBStreamServiceServant servant = new PBStreamServiceServant();
		Server server = NettyServerBuilder.forPort(m_port)
											.addService(servant)
											.build();
		server.start();

		String host = NetUtils.getLocalHostAddress();
		System.out.printf("started: StreamServer[host=%s, port=%d]%n", host, m_port);
		
		server.awaitTermination();
	}
}
