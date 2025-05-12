package utils.grpc.stream.client;

import java.io.IOException;
import java.io.InputStream;

import com.google.protobuf.ByteString;
import com.google.protobuf.Int64Value;

import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;

import utils.Throwables;
import utils.grpc.PBUtils;
import utils.grpc.stream.UpMessageChannel;

import proto.stream.MultiChannelUpMessage;
import proto.stream.StreamServiceGrpc;
import proto.stream.StreamServiceGrpc.StreamServiceStub;
import proto.stream.UpMessage;
import proto.stream.UploadServerCancelTest;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PBStreamServiceProxy {
	private final StreamServiceStub m_stub;

	public PBStreamServiceProxy(ManagedChannel channel) {
		m_stub = StreamServiceGrpc.newStub(channel);
	}
	
	public InputStream download(String path) throws IOException {
		StreamDownloadReceiver downloader = new StreamDownloadReceiver();

		// start download by sending 'stream-download' request
		StreamObserver<UpMessage> outChannel = m_stub.download(downloader);
		downloader.start(PBUtils.BYTE_STRING(path), outChannel);
		
		return downloader.getDownloadStream();
	}
	
	public long upload(String path, InputStream is) {
		try {
			StreamUploadSender uploader = new StreamUploadSender(PBUtils.BYTE_STRING(path), is);
			uploader.setChannel(m_stub.upload(uploader));
			
			ByteString ret = uploader.run();
			return Int64Value.parseFrom(ret).getValue();
		}
		catch ( Throwable e ) {
			Throwable cause = Throwables.unwrapThrowable(e);
			Throwables.sneakyThrow(cause);
			return 1;
		}
	}
	
	public StreamUploadOutputStream openUploadOutputStream(String path) {
		StreamUploadOutputStream suos = new StreamUploadOutputStream(PBUtils.BYTE_STRING(path));
		suos.setOutgoingChannel(m_stub.upload(suos));
		
		return suos;
	}

	public InputStream upAndDownload(String path, InputStream is) {
		try {
			StreamUploadSender uploader = new StreamUploadSender(PBUtils.BYTE_STRING(path), is);
			StreamDownloadReceiver downloader = new StreamDownloadReceiver();
			MultiChannelDownStreamObserver inChannel
									= new MultiChannelDownStreamObserver(uploader, downloader);
			StreamObserver<MultiChannelUpMessage> outChannel = m_stub.upAndDownload(inChannel);

			uploader.setChannel(new UpMessageChannel(outChannel, 0));
			uploader.start();

			downloader.start(new UpMessageChannel(outChannel, 1));
			return downloader.getDownloadStream();
		}
		catch ( Throwable e ) {
			Throwable cause = Throwables.unwrapThrowable(e);
			Throwables.sneakyThrow(cause);
			return null;
		}
	}
	
	public long uploadServerCancel(String path, InputStream is, int offset) {
		try {
			ByteString header =  UploadServerCancelTest.newBuilder()
														.setPath(path)
														.setOffset(offset)
														.build()
														.toByteString();
			StreamUploadSender uploader = new StreamUploadSender(header, is);
			StreamObserver<UpMessage> channel = m_stub.uploadServerCancel(uploader);
			uploader.setChannel(channel);
			
			ByteString ret = uploader.run();
			return Int64Value.parseFrom(ret).getValue();
		}
		catch ( Throwable e ) {
			Throwable cause = Throwables.unwrapThrowable(e);
			Throwables.sneakyThrow(cause);
			return 1;
		}
	}

//	public InputStream upndownFail0(String path, InputStream is) {
//		try {
//			StreamUpnDownloadClient client = new StreamUpnDownloadClient(is, m_executor) {
//				@Override
//				protected ByteString getHeader() throws Exception {
//					return PBUtils.toStringProto(path).toByteString();
//				}
//			};
//			
//			return client.upAndDownload(m_stub.upndownFail0(client));
//		}
//		catch ( Throwable e ) {
//			Throwable cause = Throwables.unwrapThrowable(e);
//			Throwables.sneakyThrow(cause);
//			return null;
//		}
//	}
}
