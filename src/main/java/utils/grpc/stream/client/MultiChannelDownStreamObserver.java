package utils.grpc.stream.client;

import io.grpc.stub.StreamObserver;

import utils.grpc.PBUtils;

import proto.stream.DownMessage;
import proto.stream.MultiChannelDownMessage;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class MultiChannelDownStreamObserver implements StreamObserver<MultiChannelDownMessage> {
	private final StreamObserver<DownMessage>[] m_channels;
	
	MultiChannelDownStreamObserver(StreamObserver<DownMessage>... channels) {
		m_channels = channels;
	}

	@Override
	public void onNext(MultiChannelDownMessage msg) {
		StreamObserver<DownMessage> channel = m_channels[msg.getChannelId()];
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
		for ( StreamObserver<DownMessage> channel: m_channels ) {
			channel.onError(cause);
		}
	}

	@Override
	public void onCompleted() {
		for ( StreamObserver<DownMessage> channel: m_channels ) {
			channel.onCompleted();
		}
	}

}
