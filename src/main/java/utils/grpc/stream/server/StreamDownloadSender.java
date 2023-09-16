package utils.grpc.stream.server;

import java.io.InputStream;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.GuardedBy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

import io.grpc.stub.StreamObserver;
import proto.stream.DownMessage;
import proto.stream.UpMessage;
import utils.Throwables;
import utils.UnitUtils;
import utils.Utilities;
import utils.async.Guard;
import utils.grpc.PBUtils;
import utils.io.IOUtils;
import utils.io.LimitedInputStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class StreamDownloadSender implements Runnable, StreamObserver<UpMessage> {
	private static final Logger s_logger = LoggerFactory.getLogger(StreamDownloadSender.class);

	private static final int DEFAULT_CHUNK_SIZE = (int)UnitUtils.parseByteSize("64kb");
	private static final long DEFAULT_STREAM_ACQUIRE_TIMEOUT = UnitUtils.parseDurationMillis("10s");	// 10 seconds
	
	private final StreamObserver<DownMessage> m_channel;
	private long m_streamAcquireTimeout = DEFAULT_STREAM_ACQUIRE_TIMEOUT;
	private int m_chunkSize = DEFAULT_CHUNK_SIZE;
	private final CountDownLatch m_startLatch = new CountDownLatch(1);

	private final Guard m_guard = Guard.create();
	@GuardedBy("m_guard") private InputStream m_stream = null;
	@GuardedBy("m_guard") private State m_state = State.WAIT_STREAM;
	@GuardedBy("m_guard") private Throwable m_cause;
	
	private static enum State {
		WAIT_STREAM,
		DOWNLOADING,
		COMPLETED,
		CANCELLED,
		FAILED,
	}
	
	protected InputStream getStream(ByteString header) throws Exception { 
		throw new AssertionError("this method should be replaced at subclass");
	}
	
	/**
	 * 주어진 InputStream을 download시킨다.
	 * Download 대상이되는 스트림은 다음과 같은 두가지 경우가 있다.
	 * <ul>
	 * 	<li> 클라이언트에서 'HEADER' 메시지가 도착한 경우, {@link #getStream(ByteString)} 메소드를
	 * 		호출하여 결과로 받은 스트림을 획득한다.
	 * 	<li> 호출자가 명시적으로 {@link #setInputStream(InputStream)}를 호출하여 설정된 경우.
	 * </ul>
	 * 
	 * @param channel	Download시킬 데이터를 전송할 출력 채널.
	 */
	public StreamDownloadSender(StreamObserver<DownMessage> channel) {
		Utilities.checkNotNullArgument(channel, "Download message channel");
		
		m_channel = channel;
	}
	
	public StreamDownloadSender getStreamTimeout(long timeout) {
		Utilities.checkArgument(timeout > 0, "invalid get-stream-timeout: " + timeout);
		
		m_streamAcquireTimeout = timeout;
		return this;
	}
	
	// download receiver의 요청에 의해 stream이 생성된 것이 아니라,
	// 로컬에서 stream 을 생성하여 설정하는 경우.
	//
	void setInputStream(InputStream stream) {
		Utilities.checkNotNullArgument(stream, "Download stream");
		
		m_guard.lock();
		try {
			if ( m_state != State.WAIT_STREAM ) {
				throw new IllegalStateException("state=" + m_state + ", expected=" + State.WAIT_STREAM);
			}
			
			m_startLatch.countDown();
			m_stream = stream;
			m_state = State.DOWNLOADING;
			m_guard.signalAll();
		}
		finally {
			m_guard.unlock();
		}
	}
	
	public StreamDownloadSender chunkSize(int size) {
		Preconditions.checkArgument(size > 0, "chunkSize > 0");
		
		m_chunkSize = size;
		return this;
	}

	@Override
	public void run() {
		try {
			// Download시킬 스트림이 준비될 때까지 대기한다. (즉 헤더가 도착할 때까지)
			// 단, 클라이언트 측에서 헤더 정보가 오지 않을 수도 있기 때문에 timeout를 설정한다.
			if ( !m_startLatch.await(m_streamAcquireTimeout, TimeUnit.MILLISECONDS) ) {
				m_guard.lock();
				try {
					if ( m_state == State.WAIT_STREAM ) {
						Exception cause = new TimeoutException("timeout while getting target input stream");
						m_channel.onNext(DownMessage.newBuilder().setError(PBUtils.ERROR(cause)).build());
						m_channel.onCompleted();
					}
					return;
				}
				finally {
					m_guard.unlock();
				}
			}
			
			// chunk를 보내는 과정 중에 자체적으로 또는 상대방쪽에서
			// 오류가 발생되거나, 취소시킬 수 있으니 확인한다.
			int chunkCount = 0;
			while ( true ) {
				// IO는 시간이 오래 걸릴 수 있기 때문에, m_guard를 잡지 않은 상태에서 수행한다.
				LimitedInputStream chunkedStream = new LimitedInputStream(m_stream, m_chunkSize);
				ByteString block = ByteString.readFrom(chunkedStream);
				
				m_guard.lock();
				try {
					if ( m_state != State.DOWNLOADING ) {
						break;
					}
					
					if ( block.isEmpty() ) {
						s_logger.trace("send END_OF_STREAM");
						DownMessage eos = DownMessage.newBuilder().setEos(PBUtils.VOID()).build();
						m_channel.onNext(eos);

						s_logger.debug("COMPLETED");
						m_channel.onCompleted();
						
						m_state = State.COMPLETED;
						m_guard.signalAll();
						break;
					}
					else {
						m_channel.onNext(DownMessage.newBuilder().setBlock(block).build());
						++chunkCount;
						
						s_logger.trace("send BLOCK[idx={}, size={}]", chunkCount, block.size());
					}
				}
				finally {
					m_guard.unlock();
				}
			}
		}
		catch ( Throwable e ) {
			Throwable cause = Throwables.unwrapThrowable(e);
			s_logger.info("local failure: " + cause);
			
			runIfDownloading(() -> {
				m_channel.onNext(DownMessage.newBuilder().setError(PBUtils.ERROR(cause)).build());
				m_channel.onCompleted();
			});
		}
		finally {
			IOUtils.closeQuietly(m_stream);
		}
	}

	@Override
	public void onNext(UpMessage msg) {
		switch ( msg.getEitherCase() ) {
			case HEADER:
				// 클라이언트로부터 stream download 요청을 받은 경우
				try {
					m_stream = getStream(msg.getHeader());
					m_guard.run(() -> m_state = State.DOWNLOADING);
				}
				catch ( Exception e ) {
					m_channel.onNext(PBUtils.EMPTY_DOWN_MESSAGE);
					
					Throwable cause = Throwables.unwrapThrowable(e);
					m_guard.run(() -> {
						m_state = State.FAILED;
						m_cause = cause;
					});
				}
				// stream을 download하는 비동기 작업을 시작시킨다.
				m_startLatch.countDown();
				break;
			case DUMMY: break;
			case ERROR:
				handleRemoteException(PBUtils.toException(msg.getError()));
				break;
			default: throw new AssertionError();
		}
	}

	@Override
	public void onCompleted() {
		handleRemoteException(new CancellationException());
	}

	@Override
	public void onError(Throwable cause) {
		handleRemoteException(cause);
	}
	
	@Override
	public String toString() {
		return String.format("%s", m_state);
	}
	
	private void handleRemoteException(Throwable cause) {
		// 원격에서 유발된 오류는 'm_state' 값만 바꾼다.
		if ( cause instanceof CancellationException ) {
			runIfDownloading(() -> {
				s_logger.debug("the peer cancelled the operation");
				
				m_channel.onNext(PBUtils.EMPTY_DOWN_MESSAGE);
				m_channel.onCompleted();
				
				m_state = State.CANCELLED;
				s_logger.info("CANCELLED by the client");
			});
		}
		else {
			runIfDownloading(() -> {
				s_logger.debug("received ERROR[cause=" + cause + "]");
				
				m_channel.onNext(PBUtils.EMPTY_DOWN_MESSAGE);
				m_channel.onCompleted();
				
				m_state = State.FAILED;
				s_logger.info("FAILED by the client");
			});
		}
		
		// header를 기다리고 있는 중일 수도 있기 때문에, latch도 down시킨다.
		m_startLatch.countDown();
	}
	
	private void runIfDownloading(Runnable action) {
		m_guard.runAndSignalAll(() -> {
			if ( m_state == State.DOWNLOADING || m_state == State.WAIT_STREAM ) {
				action.run();
			}
		});
	}
}
