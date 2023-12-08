package utils.grpc.stream.server;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CancellationException;

import javax.annotation.concurrent.GuardedBy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import io.grpc.stub.StreamObserver;
import proto.stream.DownMessage;
import proto.stream.UpMessage;
import utils.UnitUtils;
import utils.Utilities;
import utils.async.AbstractThreadedExecution;
import utils.async.Guard;
import utils.async.AsyncResult;
import utils.grpc.PBUtils;
import utils.io.StreamClosedException;
import utils.io.SuppliableInputStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class StreamUploadReceiver implements StreamObserver<UpMessage> {
	private static final Logger s_logger = LoggerFactory.getLogger(StreamUploadReceiver.class);
	private static final long SYNC_INTERVAL = UnitUtils.parseByteSize("4mb");

	private final SuppliableInputStream m_stream;
	private final StreamObserver<DownMessage> m_channel;
	
	private final Guard m_guard = Guard.create();
	@GuardedBy("m_guard") private State m_state = State.NOT_STARTED;
	@GuardedBy("m_guard") private volatile UploadedStreamProcessor m_streamConsumer = null;
	@GuardedBy("m_guard") private long m_nreceived = 0;
	@GuardedBy("m_guard") private long m_lastSync = 0;
	
	private static enum State {
		NOT_STARTED,
		UPLOADING,
		UPLOAD_FINISHED,	// upload client에서 모든 데이터가 모두 도착한 경우
		COMPLETED,			// result까지 보낸 상태
		CANCELLED,			// upload client가 중간에 멈춘 경우 최종 상태
		FAILED,
	}
	
	/**
	 * @param req	스트림 처리에 필요한 인자 정보들이 serialize된 바이트 배열.
	 * @param is	처리에 필요한 스트림.
	 * @return Upload된 스트림 처리 결과. 별도의 결과가 없는 경우는 {@code null}을 반환함.
	 */
	abstract protected ByteString consumeStream(ByteString req, InputStream is) throws Exception;
	
	public StreamUploadReceiver(StreamObserver<DownMessage> channel) {
		Utilities.checkNotNullArgument(channel);
		
		m_stream = SuppliableInputStream.create();
		m_channel = channel;
	}

	@Override
	public void onNext(UpMessage msg) {
		switch ( msg.getEitherCase() ) {
			case BLOCK:
				onBlockReceived(msg.getBlock());
				break;
			case HEADER:
				onHeaderReceived(msg.getHeader());
				break;
			case EOS:
				onEosReceived();
				break;
			case ERROR:
				handleRemoteException(PBUtils.toException(msg.getError()));
				break;
			case DUMMY: break;
			default:
				throw new AssertionError("upward message: " + msg.getEitherCase());
		}
	}
	
	@Override
	public void onError(Throwable cause) {
		handleRemoteException(cause);
	}

	@Override
	public void onCompleted() {
		s_logger.info("received DISCONNECTION");
		handleRemoteException(new CancellationException());
	}
	
	@Override
	public String toString() {
		return String.format("%s[%s]", getClass().getSimpleName(), m_state);
	}
	
	private void onHeaderReceived(ByteString header) {
		s_logger.info("received HEADER: {}", header);
		// 'handleStream()' 메소드가 실제 모든 작업이 종료되기 전에 반환될 수도 있기 때문에
		// 'StreamHandler' 비동기 연산이 완료(completed)되었다고 무조건 'onCompleted()' 메소드를
		// 호출하면 안된다.
		// 그러므로 비동기 연산이 종료되고, client쪽에서 stream이 모두 올라온 시점에서
		// onCompleted() 메소드를 호출한다.
		// 그러나 만일 비동기 연산이 실패이거나 취소인 경우는 바로 'onCompleted()'를 호출해도
		// 무방하다.
		//
		m_guard.run(() -> {
			if ( m_state == State.NOT_STARTED ) {
				m_streamConsumer = new UploadedStreamProcessor(header);
				m_streamConsumer.whenFinished(this::onConsumerFinished);
				m_streamConsumer.start();
				
				m_state = State.UPLOADING;
				m_guard.signalAll();
			}
		});
	}
	
	private void onBlockReceived(ByteString chunk) {
		s_logger.trace("received BLOCK[size={}]", chunk.size());
		
		m_guard.lock();
		try {
			if ( m_state == State.UPLOADING ) {
				m_stream.supply(chunk.asReadOnlyByteBuffer());
				
				m_nreceived += chunk.size();
				long sync = m_nreceived / SYNC_INTERVAL;
				if ( sync != m_lastSync ) {
					m_channel.onNext(DownMessage.newBuilder().setOffset(m_nreceived).build());
					m_lastSync = sync;
					System.out.println(m_nreceived);
				}
			}
			else {
				s_logger.trace("late stream block: state=" + m_state);
			}
		}
		catch ( StreamClosedException  e ) {
			// 이미 stream이 close된 경우. consumer 동작결과에 따라 최종상태가 결정됨
		}
		catch ( InterruptedException e ) {
			m_channel.onNext(DownMessage.newBuilder()
										.setError(PBUtils.ERROR(new CancellationException()))
										.build());
			m_channel.onCompleted();
			
			m_state = State.CANCELLED;
			m_guard.signalAll();
			s_logger.info("CANCELLED by the stream consumer");
			
			m_streamConsumer.cancel(true);
		}
		catch ( Exception e ) {
			throw new AssertionError("unexpected exception: " + e);
		}
		finally {
			m_guard.unlock();
		}
	}
	
	private void onEosReceived() {
		// 클라이언트측에서 모든 데이터를 upload시킨 경우
		s_logger.debug("received: EOS");

		m_guard.runAndSignalAll(() -> {
			if ( m_state == State.UPLOADING ) {
				m_stream.endOfSupply();
				
				m_state = State.UPLOAD_FINISHED;
				m_guard.signalAll();
			}
		});
	}
	
	private void handleRemoteException(Throwable cause) {
		m_guard.runAndSignalAll(() -> {
			if ( m_state == State.UPLOADING || m_state == State.UPLOAD_FINISHED ) {
				// 클라이언트측에서 모든 데이터를 upload시킨 경우
				if ( cause instanceof CancellationException ) {
					s_logger.info("The peer has cancelled the operation");
					m_state = State.CANCELLED;
					
					m_stream.endOfSupply(new IOException("cancelled"));
				}
				else {
					s_logger.warn("received ERROR: " + cause);
					
					m_state = State.FAILED;
					m_stream.endOfSupply(new IOException("data is not available: " + cause));
				}
				
				m_channel.onNext(PBUtils.EMPTY_DOWN_MESSAGE);
				m_channel.onCompleted();
			}
		});
	}
	
	private void onConsumerFinished(AsyncResult<ByteString> result) {
		m_guard.runAndSignalAll(() -> {
			if ( m_state == State.COMPLETED || m_state == State.CANCELLED || m_state == State.FAILED ) {
				return;	// skip
			}
			
			if ( result.isCompleted() ) {
				s_logger.debug("stream consumer finished: result: {}", result.getUnchecked());

				m_channel.onNext(DownMessage.newBuilder().setResult(result.getUnchecked()).build());
				m_channel.onCompleted();
				m_state = State.COMPLETED;
			}
			else if ( result.isFailed() ) {
				s_logger.debug("stream consumer failed: cause: {}", "" + result.getCause());
				
				m_channel.onNext(DownMessage.newBuilder()
											.setError(PBUtils.ERROR(result.getCause()))
											.build());
				m_channel.onCompleted();
				m_state = State.FAILED;
			}
			else if ( result.isCancelled() ) {
				s_logger.debug("stream consumer cancelled the operation");
				
				m_channel.onNext(DownMessage.newBuilder()
											.setError(PBUtils.ERROR(new CancellationException()))
											.build());
				m_channel.onCompleted();
				m_state = State.CANCELLED;
			}
		});
	}
	
	private class UploadedStreamProcessor extends AbstractThreadedExecution<ByteString> {
		private final ByteString m_req;
		
		private UploadedStreamProcessor(ByteString req) {
			m_req = req;
		}
		
		@Override
		protected ByteString executeWork() throws Exception {
			return consumeStream(m_req, m_stream);
		}
	}
}