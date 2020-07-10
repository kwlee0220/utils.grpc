package utils.grpc.stream.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.StringValue;

import io.grpc.stub.StreamObserver;
import proto.stream.DownMessage;
import proto.stream.MultiChannelDownMessage;
import proto.stream.MultiChannelUpMessage;
import proto.stream.StreamServiceGrpc.StreamServiceImplBase;
import proto.stream.UpMessage;
import proto.stream.UploadServerCancelTest;
import utils.UnitUtils;
import utils.grpc.PBUtils;
import utils.grpc.stream.DownMessageChannel;
import utils.io.IOUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PBStreamServiceServant extends StreamServiceImplBase {
	private static final Logger s_logger = LoggerFactory.getLogger(PBStreamServiceServant.class);
	
	private final ExecutorService m_executor = Executors.newCachedThreadPool();

	@Override
    public StreamObserver<UpMessage> download(StreamObserver<DownMessage> channel) {
		StreamDownloadSender sender = new StreamDownloadSender(channel) {
			@Override
			protected InputStream getStream(ByteString req) throws Exception {
				String path = PBUtils.STRING(req);
				
				File file = new File(path);
				int nbytes = -1;
				try {
					nbytes = Integer.parseInt(file.getName());
					file = file.getParentFile();
				}
				catch ( Exception e ) { }

				InputStream is = new FileInputStream(file);
				if ( nbytes >= 0 ) {
					s_logger.trace("download: path={}, length={}", file.getPath(), nbytes);
					return new FailingInputStream(is, nbytes);
				}
				else {
					s_logger.trace("download: path={}", path);
					return is;
				}
			}
		};
		CompletableFuture.runAsync(sender, m_executor);
		
		return sender;
	}
	
	private static class FailingInputStream extends InputStream {
		private final InputStream m_is;
		private int m_remains;
		
		FailingInputStream(InputStream is, int remains) {
			m_is = is;
			m_remains = remains;
		}

		@Override
		public int read() throws IOException {
			if ( --m_remains >= 0 ) {
				return m_is.read();
			}
			else {
				throw new IOException("test failure");
			}
		}
		
		@Override
	    public int read(byte b[], int off, int len) throws IOException {
			if ( m_remains > 0 ) {
				len = Math.min(len, m_remains);
				int nbytes = m_is.read(b, off, len);
				m_remains -= nbytes;
				
				return nbytes;
			}
			else {
				throw new IOException("test failure");
			}
		}
	}
	
	private static final int BUFFER_SIZE = (int)UnitUtils.parseByteSize("4mb");

	@Override
    public StreamObserver<UpMessage> upload(StreamObserver<DownMessage> channel) {
		return new StreamUploadReceiver(channel) {
			@Override
			protected ByteString consumeStream(ByteString header, InputStream is) throws Exception {
				String path = StringValue.parseFrom(header).getValue();
				File file = new File(path);

				s_logger.debug("[sample] uploading: path={}...", path);
				
				long count = IOUtils.toFile(is, file, BUFFER_SIZE);
				System.out.println("length=" + count);
				return PBUtils.INT64(count).toByteString();
			}
		};
	}

	@Override
    public StreamObserver<UpMessage>
	uploadServerCancel(StreamObserver<DownMessage> channel) {
		return new StreamUploadReceiver(channel) {
			@Override
			protected ByteString consumeStream(ByteString header, InputStream is)
				throws Exception {
				UploadServerCancelTest test = UploadServerCancelTest.parseFrom(header);
				
				File file = new File(test.getPath());
				s_logger.debug("upload: path={}...", file);
				
				byte[] buf = new byte[test.getOffset()];
				IOUtils.readFully(is, buf);
				is.close();
				
				return PBUtils.INT64(test.getOffset()).toByteString();
			}
		};
	}

	@Override
    public StreamObserver<MultiChannelUpMessage>
	upAndDownload(StreamObserver<MultiChannelDownMessage> out) {
		DownMessageChannel upload = new DownMessageChannel(out, 0);
		DownMessageChannel download = new DownMessageChannel(out, 1);
		
		StreamDownloadSender downloader = new StreamDownloadSender(download);
		StreamUploadReceiver uploader = new StreamUploadReceiver(upload) {
			@Override
			protected ByteString consumeStream(ByteString header, InputStream is)
				throws Exception {
				String path = StringValue.parseFrom(header).getValue();

				s_logger.debug("[sample] uploading: path={}...", path);
				downloader.setInputStream(is);
				downloader.run();
				
				return PBUtils.VOID().toByteString();
			}
		};
		
		return new MultiChannelUpStreamObserver(uploader, downloader);
	}
}
