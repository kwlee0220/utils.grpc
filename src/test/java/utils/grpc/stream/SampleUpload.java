package utils.grpc.stream;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.log4j.PropertyConfigurator;

import utils.grpc.stream.client.PBStreamServiceProxy;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleUpload {
//	private static final String INPUT = "/mnt/data/sbdata/data/기타/유엔진/2018/11.csv";
	private static final String INPUT = "/mnt/data/sbdata/data/공공데이터포털/주유소_가격/주유소_가격.csv";
	private static final String OUTPUT = "/home/kwlee/tmp/copy";
			
	public static final void main(String... args) throws Exception {
		PropertyConfigurator.configure("log4j.properties");
//		LogManager.getRootLogger().setLevel(Level.OFF);
		
		PBStreamClient client = PBStreamClient.connect("localhost", 15685);
		PBStreamServiceProxy proxy = client.getStreamService();

		try ( InputStream is = new FileInputStream(new File(INPUT)) ) {
			System.out.println("nbytes=" + client.upload(OUTPUT, is));
		}
		new File(OUTPUT).delete();
		System.out.println("----------------------------------------------------------");

		try ( InputStream src = new FileInputStream(new File(INPUT));
				InputStream is = new FailingInputStream(src, 0, new IOException("test"))) {
			System.out.println("nbytes=" + client.upload(OUTPUT, is));
		}
		catch ( Exception e ) {
			System.out.println("" + e);
		}
		new File(OUTPUT).delete();
		System.out.println("----------------------------------------------------------");

		try ( InputStream src = new FileInputStream(new File(INPUT));
				InputStream is = new FailingInputStream(src, 45678, new IOException("test"))) {
			System.out.println("nbytes=" + client.upload(OUTPUT, is));
		}
		catch ( Exception e ) {
			System.out.println("" + e);
		}
		new File(OUTPUT).delete();
		System.out.println("----------------------------------------------------------");

		try ( InputStream src = new FileInputStream(new File(INPUT));
				InputStream is = new FailingInputStream(src, 435678, new IOException("test"))) {
			System.out.println("nbytes=" + client.upload(OUTPUT, is));
		}
		catch ( Exception e ) {
			System.out.println("" + e);
		}
		new File(OUTPUT).delete();
		System.out.println("----------------------------------------------------------");

		try ( InputStream is = new FileInputStream(new File(INPUT)) ) {
			System.out.println("nbytes=" + proxy.uploadServerCancel(OUTPUT, is, 128767));
		}
		catch ( Exception e ) {
			System.out.println("" + e);
		}
		System.out.println("----------------------------------------------------------");

		try ( InputStream is = new FileInputStream(new File(INPUT)) ) {
			System.out.println("nbytes=" + proxy.uploadServerCancel(OUTPUT, is, 3287679));
		}
		catch ( Exception e ) {
			System.out.println("" + e);
		}
		System.out.println("----------------------------------------------------------");
		
		Thread.sleep(1000);
	}
}
