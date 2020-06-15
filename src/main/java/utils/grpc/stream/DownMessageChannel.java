package utils.grpc.stream;

import io.grpc.stub.StreamObserver;
import proto.stream.DownMessage;
import proto.stream.MessageType;
import proto.stream.MultiChannelDownMessage;
import utils.grpc.PBUtils;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DownMessageChannel implements StreamObserver<DownMessage> {
	private final StreamObserver<MultiChannelDownMessage> m_channel;
	private final int m_channelId;

	public DownMessageChannel(StreamObserver<MultiChannelDownMessage> channel, int channelId) {
		m_channel = channel;
		m_channelId = channelId;
	}

	@Override
	public void onNext(DownMessage msg) {
		synchronized ( m_channel ) {
			m_channel.onNext(MultiChannelDownMessage.newBuilder()
													.setChannelId(m_channelId)
													.setType(MessageType.DATA)
													.setMsg(msg)
													.build());
		}
	}

	@Override
	public void onError(Throwable cause) {
		synchronized ( m_channel ) {
			DownMessage msg = DownMessage.newBuilder().setError(PBUtils.ERROR(cause)).build();
			m_channel.onNext(MultiChannelDownMessage.newBuilder()
													.setChannelId(m_channelId)
													.setType(MessageType.GRPC_ERROR)
													.setMsg(msg)
													.build());
		}
	}

	@Override
	public void onCompleted() {
		synchronized ( m_channel ) {
			m_channel.onNext(MultiChannelDownMessage.newBuilder()
													.setChannelId(m_channelId)
													.setType(MessageType.CLOSE)
													.build());
		}
	}
}