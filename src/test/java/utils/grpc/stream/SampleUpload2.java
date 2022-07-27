package utils.grpc.stream;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

import com.google.protobuf.ByteString;

import utils.StopWatch;
import utils.grpc.stream.client.PBStreamServiceProxy;
import utils.grpc.stream.client.StreamUploadOutputStream;
import utils.io.IOUtils;

import proto.Int64Proto;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleUpload2 {
//	private static final String INPUT = "/mnt/data/sbdata/data/기타/유엔진/2018/11.csv";
//	private static final String INPUT = "/mnt/data/sbdata/공공데이터포털/주유소_가격/주유소_가격.csv";
	private static final String INPUT = "/home/kwlee/tmp/datastore/구역/연속지적도_2017.avro";
//	private static final String INPUT = "/home/kwlee/tmp/datastore/건물/건물통합정보마스터.avro";
	private static final String OUTPUT = "/home/kwlee/tmp/copy";
			
	public static final void main(String... args) throws Exception {
		PBStreamClient client = PBStreamClient.connect("localhost", 15685);
		PBStreamServiceProxy proxy = client.getStreamService();
		
		StopWatch watch = StopWatch.start();
		long nsent;
		StreamUploadOutputStream suos = client.openUploadOutputStream(OUTPUT); 
		try ( InputStream is = new FileInputStream(new File(INPUT)) ) {
			nsent = IOUtils.transfer(is, suos, 256 * 1024);
		}
		suos.close();
			
		ByteString ret = suos.awaitResult();
		long nreceived = Int64Proto.parseFrom(ret).getValue();
		watch.stop();
		System.out.printf("nsent=%d, nreceived=%d, elapsed=%d%n", nsent, nreceived, watch.getElapsedInMillis());

//		try ( InputStream is = new FileInputStream(new File(INPUT)) ) {
//			System.out.println("nbytes=" + client.upload(OUTPUT, is));
//		}
//		new File(OUTPUT).delete();
//		System.out.println("----------------------------------------------------------");
//
//		try ( InputStream src = new FileInputStream(new File(INPUT));
//				InputStream is = new FailingInputStream(src, 0, new IOException("test"))) {
//			System.out.println("nbytes=" + client.upload(OUTPUT, is));
//		}
//		catch ( Exception e ) {
//			System.out.println("" + e);
//		}
//		new File(OUTPUT).delete();
//		System.out.println("----------------------------------------------------------");
//
//		try ( InputStream src = new FileInputStream(new File(INPUT));
//				InputStream is = new FailingInputStream(src, 45678, new IOException("test"))) {
//			System.out.println("nbytes=" + client.upload(OUTPUT, is));
//		}
//		catch ( Exception e ) {
//			System.out.println("" + e);
//		}
//		new File(OUTPUT).delete();
//		System.out.println("----------------------------------------------------------");
//
//		try ( InputStream src = new FileInputStream(new File(INPUT));
//				InputStream is = new FailingInputStream(src, 435678, new IOException("test"))) {
//			System.out.println("nbytes=" + client.upload(OUTPUT, is));
//		}
//		catch ( Exception e ) {
//			System.out.println("" + e);
//		}
//		new File(OUTPUT).delete();
//		System.out.println("----------------------------------------------------------");
//
//		try ( InputStream is = new FileInputStream(new File(INPUT)) ) {
//			System.out.println("nbytes=" + proxy.uploadServerCancel(OUTPUT, is, 128767));
//		}
//		catch ( Exception e ) {
//			System.out.println("" + e);
//		}
//		System.out.println("----------------------------------------------------------");
//
//		try ( InputStream is = new FileInputStream(new File(INPUT)) ) {
//			System.out.println("nbytes=" + proxy.uploadServerCancel(OUTPUT, is, 3287679));
//		}
//		catch ( Exception e ) {
//			System.out.println("" + e);
//		}
//		System.out.println("----------------------------------------------------------");
		
		Thread.sleep(1000);
	}
}
