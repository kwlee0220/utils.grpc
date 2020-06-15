package utils.grpc.stream;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

import org.apache.log4j.PropertyConfigurator;

import utils.grpc.stream.client.PBStreamServiceProxy;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleUploadAndDownload {
	private static final String INPUT = "/mnt/data/sbdata/data/나비콜/201601/carloc_hst-20160124.dat";
//	private static final String INPUT = "/mnt/data/sbdata/data/공공데이터포털/주유소_가격/주유소_가격.csv";
	private static final String OUTPUT = "/home/kwlee/tmp/copy_server";
	private static final String OUTPUT_CLIENT = "/home/kwlee/tmp/copy_client";
			
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
//		LogManager.getRootLogger().setLevel(Level.OFF);
		
		PBStreamClient client = PBStreamClient.connect("localhost", 15685);
		PBStreamServiceProxy proxy = client.getStreamService();

//		try ( InputStream is = new FileInputStream(new File(INPUT));
//				InputStream out = proxy.upAndDownload(OUTPUT, is) ) {
//			IOUtils.toFile(out, new File(OUTPUT_CLIENT));
//		}
//		System.out.printf("%d -> %d%n", new File(INPUT).length(), new File(OUTPUT_CLIENT).length());
//		new File(OUTPUT).delete();

		try ( InputStream is = new FileInputStream(new File(INPUT));
				InputStream out = proxy.upAndDownload(OUTPUT, is) ) {
			byte[] buf = new byte[64 * 1024];
			for ( int i =1; i < 132; ++i ) {
				int nbytes = out.read(buf);
				if ( nbytes < 0 ) {
					break;
				}
			}
			out.close();
		}
		System.out.println("----------------------------------------------------------");

//		try ( InputStream is = new FileInputStream(new File(INPUT));
//				InputStream out = proxy.upndownFail0(OUTPUT, is) ) {
//			byte[] buf = new byte[64 * 1024];
//			for ( int i =1; true; ++i ) {
//				int nbytes = out.read(buf);
//				if ( nbytes < 0 ) {
//					break;
//				}
//			}
//		}
//		catch ( Exception e ) {
//			System.out.println(e);
//		}
//		System.out.println("----------------------------------------------------------");
	}
}
