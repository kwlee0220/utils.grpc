package utils.grpc.stream.server;

import io.grpc.stub.StreamObserver;
import proto.stream.MultiChannelUpMessage;
import proto.stream.UpMessage;
import utils.grpc.PBUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class MultiChannelUpStreamObserver implements StreamObserver<MultiChannelUpMessage> {
	private final StreamObserver<UpMessage>[] m_channels;
	
	MultiChannelUpStreamObserver(StreamObserver<UpMessage>... channels) {
		m_channels = channels;
	}

	@Override
	public void onNext(MultiChannelUpMessage msg) {
		StreamObserver<UpMessage> channel = m_channels[msg.getChannelId()];
		switch ( msg.getType() ) {
			case DATA:
				channel.onNext(msg.getMsg());
				break;
			case GRPC_ERROR:
				channel.onError(PBUtils.toException(msg.getMsg().getError()));
				break;
			case CLOSE:
				channel.onCompleted();
				break;
			default: throw new AssertionError();
		}
	}

	@Override
	public void onError(Throwable cause) {
		for ( StreamObserver<UpMessage> channel: m_channels ) {
			channel.onError(cause);
		}
	}

	@Override
	public void onCompleted() {
		for ( StreamObserver<UpMessage> channel: m_channels ) {
			channel.onCompleted();
		}
	}

}
