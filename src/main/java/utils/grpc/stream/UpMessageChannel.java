package utils.grpc.stream;

import io.grpc.stub.StreamObserver;

import utils.grpc.PBUtils;

import proto.stream.MessageType;
import proto.stream.MultiChannelUpMessage;
import proto.stream.UpMessage;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class UpMessageChannel implements StreamObserver<UpMessage> {
	private final StreamObserver<MultiChannelUpMessage> m_channel;
	private final int m_subChannelId;

	public UpMessageChannel(StreamObserver<MultiChannelUpMessage> channel, int subChannelId) {
		m_channel = channel;
		m_subChannelId = subChannelId;
	}

	@Override
	public void onNext(UpMessage msg) {
		synchronized ( m_channel ) {
			m_channel.onNext(MultiChannelUpMessage.newBuilder()
												.setChannelId(m_subChannelId)
												.setType(MessageType.DATA)
												.setMsg(msg)
												.build());
		}
	}

	@Override
	public void onError(Throwable cause) {
		UpMessage msg = UpMessage.newBuilder().setError(PBUtils.ERROR(cause)).build();
		synchronized ( m_channel ) {
			m_channel.onNext(MultiChannelUpMessage.newBuilder()
												.setChannelId(m_subChannelId)
												.setType(MessageType.GRPC_ERROR)
												.setMsg(msg)
												.build());
		}
	}

	@Override
	public void onCompleted() {
		synchronized ( m_channel ) {
			m_channel.onNext(MultiChannelUpMessage.newBuilder()
												.setChannelId(m_subChannelId)
												.setType(MessageType.CLOSE)
												.build());
		}
	}
}