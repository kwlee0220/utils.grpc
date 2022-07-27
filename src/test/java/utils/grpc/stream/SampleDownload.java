package utils.grpc.stream;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Random;

import utils.StopWatch;
import utils.UnitUtils;
import utils.io.IOUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SampleDownload {
	private static final String INPUT = "/mnt/data/sbdata/data/공공데이터포털/주유소_가격/주유소_가격.csv";
	private static final String INPUT2 = "/mnt/data/sbdata/data/나비콜/201601/carloc_hst-20160124.dat";
	private static final String INPUT3 = "/mnt/data/sbdata/data/도로교통안전공단/DTG/20160901.csv.gz";
	private static final String OUTPUT = "/home/kwlee/tmp/copy";
			
	public static final void main(String... args) throws Exception {
		PBStreamClient client = PBStreamClient.connect("localhost", 15685);

		for ( String path: Arrays.asList(INPUT, INPUT2, INPUT3) ) {
			StopWatch watch = StopWatch.start();
			long nbytes = fullCopy(client, path);
			watch.stop();
			
			long velo = Math.round(nbytes / watch.getElapsedInFloatingSeconds());
			System.out.printf("ncopied=%s, velo=%s/s%n",
								UnitUtils.toByteSizeString(nbytes),
								UnitUtils.toByteSizeString(velo));
		}
		
		for ( int i = 0; i < 10; ++i ) {
			Random rand = new Random(System.currentTimeMillis());
			int count = 7 + rand.nextInt(51);
			partialCopy(client, INPUT2, count);
		}
		for ( int i = 0; i < 3; ++i ) {
			partialCopy(client, INPUT2, 0);
		}

		serverCancel(client, INPUT2, 0);
		for ( int i =0; i < 10; ++i ) {
			Random rand = new Random(System.currentTimeMillis());
			long size = (1 + rand.nextInt(51)) * 37117;
			serverCancel(client, INPUT2, size);
		}
		
		Thread.sleep(1000);
	}
	
	private static long fullCopy(PBStreamClient client, String path) throws IOException {
		try ( InputStream is = client.download(path) ) {
			return IOUtils.toFile(is, new File(OUTPUT));
		}
		finally {
			new File(OUTPUT).delete();
		}
	}
	
	private static void partialCopy(PBStreamClient client, String path, int count) throws IOException {
		try ( InputStream is = client.download(path) ) {
			byte[] buf = new byte[127897];
			for ( int i = 0; i < count; ++i ) {
				IOUtils.readAtBest(is, buf);
			}
		}
	}

	private static void serverCancel(PBStreamClient client, String path, long size) throws IOException {
		try ( InputStream is = client.download(path + "/" + size) ) {
			IOUtils.toFile(is, new File(OUTPUT));
		}
		catch ( Exception e ) { }
		new File(OUTPUT).delete();
	}
}
