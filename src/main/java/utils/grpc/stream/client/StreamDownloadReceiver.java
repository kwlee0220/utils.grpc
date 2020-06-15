package utils.grpc.stream.client;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CancellationException;

import javax.annotation.concurrent.GuardedBy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import io.grpc.stub.StreamObserver;
import proto.ErrorValue;
import proto.ErrorValue.Code;
import proto.stream.DownMessage;
import proto.stream.UpMessage;
import utils.Utilities;
import utils.async.EventDrivenExecution;
import utils.async.Guard;
import utils.grpc.PBUtils;
import utils.grpc.stream.StreamClosedException;
import utils.grpc.stream.SuppliableInputStream;


/**
 * 
 * 사용자의 request 메시지가 있는 경우 (즉, receive()에 req가 전달된 경우),
 * 해당 메시지를 전송하는 것으로 시작됨.
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class StreamDownloadReceiver	extends EventDrivenExecution<Void>
									implements StreamObserver<DownMessage> {
	private static final Logger s_logger = LoggerFactory.getLogger(StreamDownloadReceiver.class);
	
	private static final int DEFAULT_CHUNK_SLOTS = 16;
	
	private final SuppliableInputStream m_stream;
	private StreamObserver<UpMessage> m_channel;
	
	private static enum State {
		NOT_STARTED,
		DOWNLOADING,
		COMPLETED,
		CANCELLED,
		FAILED,
	}
	private final Guard m_guard = Guard.create();
	@GuardedBy("m_guard") private State m_state = State.NOT_STARTED;
	
	public StreamDownloadReceiver(int nchunks) {
		Utilities.checkArgument(nchunks >= 1, "invalid buffer size: " + nchunks);
		
		m_stream = SuppliableInputStream.create(nchunks);
	}
	
	public StreamDownloadReceiver() {
		this(DEFAULT_CHUNK_SLOTS);
	}
	
	public InputStream getDownloadStream() {
		return m_stream;
	}

	public void start(StreamObserver<UpMessage> channel) throws Throwable {
		Utilities.checkNotNullArgument(channel, "upward channel");
		
		m_guard.runOrThrow(() -> {
			if ( m_state == State.NOT_STARTED ) {
				m_channel = channel;
				m_state = State.DOWNLOADING;
				m_guard.signalAll();
				notifyStarted();
			}
			else {
				throw new IllegalStateException("already started: state=" + m_state);
			}
		});
	}

	public void start(ByteString req, StreamObserver<UpMessage> channel) {
		Utilities.checkNotNullArgument(req, "download initiation message");
		Utilities.checkNotNullArgument(channel, "out-going channel");
		
		m_guard.runOrThrow(() -> {
			if ( m_state == State.NOT_STARTED ) {
				m_channel = channel;
				m_channel.onNext(UpMessage.newBuilder().setHeader(req).build());
				m_state = State.DOWNLOADING;
				m_guard.signalAll();
				notifyStarted();
			}
			else {
				throw new IllegalStateException("already started: state=" + m_state);
			}
		});
	}

	@Override
	public void onNext(DownMessage msg) {
		switch ( msg.getEitherCase() ) {
			case BLOCK:
				onBlockReceived(msg.getBlock());
				break;
			case EOS:
				onEosReceived();
				break;
			case ERROR:
				onRemoteException(PBUtils.toException(msg.getError()));
				break;
			case DUMMY: break;
			default:
				throw new AssertionError();
		}
	}

	@Override
	public void onCompleted() {
		onRemoteException(new CancellationException());
	}
	
	@Override
	public void onError(Throwable cause) {
		onRemoteException(cause);
	}
	
	@Override
	public String toString() {
		return String.format("%s", m_state);
	}
	
	private void onBlockReceived(ByteString block) {
		if ( m_guard.get(() -> m_state != State.DOWNLOADING) ) {
			return;
		}
		
		try {
			m_stream.supply(block.asReadOnlyByteBuffer());
		}
		catch ( StreamClosedException e ) {
			m_guard.runAndSignalAll(() -> {
				if ( m_state == State.DOWNLOADING ) {
					m_channel.onNext(UpMessage.newBuilder()
												.setError(PBUtils.ERROR(Code.CANCELLED, "downloader has closed the stream"))
												.build());
					m_channel.onCompleted();
					
					m_state = State.CANCELLED;
					notifyCancelled();
					s_logger.info("CANCELLED by the application");
				}
			});
		}
		catch ( InterruptedException e ) {
			m_guard.runAndSignalAll(() -> {
				if ( m_state == State.DOWNLOADING ) {
					ErrorValue error = PBUtils.ERROR(Code.INTERNAL, "downloader worker was interrupted");
					m_channel.onNext(UpMessage.newBuilder().setError(error).build());
					m_channel.onCompleted();
					
					m_state = State.FAILED;
					notifyFailed(e);
					s_logger.info("FAILED because the application interrupt the data supplying-thread");
				}
			});
		}
	}
	
	private void onEosReceived() {
		m_guard.runAndSignalAll(() -> {
			if ( m_state == State.DOWNLOADING ) {
				m_stream.endOfSupply();
				
				// 바로 'onComplete()'를 호출하면 cancellation 오류가 발생할 수 있으므로,
				// dummy 메시지를 보내고 호출한다.
				m_channel.onNext(PBUtils.EMPTY_UP_MESSAGE);
				m_channel.onCompleted();
				
				m_state = State.COMPLETED;
				notifyCompleted(null);
				s_logger.debug("received EOS -> COMPLETED");
			}
		});
	}
	
	private void onRemoteException(Throwable cause) {
		m_guard.runAndSignalAll(() -> {
			if ( m_state == State.DOWNLOADING || m_state == State.NOT_STARTED ) {
				s_logger.warn("received ERROR: " + cause);
				if ( !(cause instanceof IOException) ) {
					m_stream.endOfSupply(new IOException("" + cause));
				}
				else {
					m_stream.endOfSupply(cause);
				}
				
				// client의 start가 너무 늦게 호출되는 경우에는 'm_channel' 조차도
				// 설정되기 이전일 수도 있기 때문에 확인한다.
				if ( m_channel != null ) {
					m_channel.onNext(PBUtils.EMPTY_UP_MESSAGE);
					m_channel.onCompleted();
				}
				
				m_state = State.FAILED;
				notifyFailed(cause);
			}
		});
	}
}