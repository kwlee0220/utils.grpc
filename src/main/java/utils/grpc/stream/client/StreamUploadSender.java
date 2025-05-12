package utils.grpc.stream.client;

import java.io.InputStream;
import java.util.Date;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.GuardedBy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import io.grpc.stub.StreamObserver;

import utils.Throwables;
import utils.UnitUtils;
import utils.Utilities;
import utils.async.AbstractThreadedExecution;
import utils.async.Guard;
import utils.grpc.PBUtils;
import utils.io.LimitedInputStream;

import proto.ErrorProto.Code;
import proto.stream.DownMessage;
import proto.stream.UpMessage;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class StreamUploadSender extends AbstractThreadedExecution<ByteString>
								implements StreamObserver<DownMessage> {
	private static final Logger s_logger = LoggerFactory.getLogger(StreamUploadSender.class);
	
	private static final int DEFAULT_CHUNK_SIZE = (int)UnitUtils.parseByteSize("64kb");
	private static final long DEFAULT_CLOSE_TIMEOUT = UnitUtils.parseDurationMillis("30s");		// 30s
	
	private final ByteString m_header;
	private final InputStream m_stream;
	private StreamObserver<UpMessage> m_channel = null;
	private int m_chunkSize = DEFAULT_CHUNK_SIZE;
	
	private final Guard m_guard = Guard.create();
	@GuardedBy("m_guard") private State m_state = State.NOT_STARTED;
	@GuardedBy("m_guard") private ByteString m_result = null;
	@GuardedBy("m_guard") private Exception m_cause = null;
	@GuardedBy("m_guard") private long m_head = 0;
	@GuardedBy("m_guard") private long m_tail = 0;
	
	private static enum State {
		NOT_STARTED,
		UPLOADING,
		END_OF_STREAM,
		CANCELLED,
		FAILED,
	}
	
	public StreamUploadSender(ByteString header, InputStream stream) {
		Utilities.checkNotNullArgument(header, "upload request header");
		Utilities.checkNotNullArgument(stream, "Stream to upload");
		
		m_header = header;
		m_stream = stream;
	}
	
	public void setChannel(StreamObserver<UpMessage> channel) {
		Utilities.checkNotNullArgument(channel, "Upload stream channel");

		m_channel = channel;
	}

	@Override
	protected ByteString executeWork() throws InterruptedException, CancellationException, Exception {
		Utilities.checkState(m_channel != null, "Upload stream channel has not been set");

		m_guard.run(() -> {
			if ( m_state == State.NOT_STARTED ) {
				s_logger.trace("send HEADER: {}", m_header);
				UpMessage req = UpMessage.newBuilder().setHeader(m_header).build();
				m_channel.onNext(req);
				
				m_state = State.UPLOADING;
				m_guard.signalAll();
			}
		});
		
		try {	
			int chunkCount = 0;
			while ( true ) {
				// IO는 시간이 오래 걸릴 수 있기 때문에, m_guard를 잡지 않은 상태에서 수행한다.
				LimitedInputStream chunkedStream = new LimitedInputStream(m_stream, m_chunkSize);
				ByteString chunk = ByteString.readFrom(chunkedStream);
				
				m_guard.lock();
				try {
					if ( m_state != State.UPLOADING || m_result != null ) {
						break;
					}

					if ( chunk.isEmpty() ) {
						s_logger.trace("send END_OF_STREAM");
						UpMessage eos = UpMessage.newBuilder().setEos(PBUtils.VOID()).build();
						m_channel.onNext(eos);
						
						m_state = State.END_OF_STREAM;
						m_guard.signalAll();
						break;
					}
					else {
						++chunkCount;
						s_logger.trace("send CHUNK[idx={}, size={}]", chunkCount, chunk.size());
						UpMessage block = UpMessage.newBuilder().setBlock(chunk).build();
						m_channel.onNext(block);
						m_head += chunk.size();
					}
					Thread.sleep(30);
				}
				finally {
					m_guard.unlock();
				}
			}
		}
		catch ( Exception e ) {
			Throwable cause = Throwables.unwrapThrowable(e);
			s_logger.info("local failure: " + cause);
			
			m_guard.run(() -> {
				if ( m_state == State.UPLOADING ) {
					m_channel.onNext(UpMessage.newBuilder().setError(PBUtils.ERROR(cause)).build());
					m_channel.onCompleted();
					
					m_state = State.FAILED;
					m_cause = e;
				}
			});
		}
		
		m_guard.lock();
		try {
			if ( m_state == State.END_OF_STREAM && m_result == null ) {
				// 모든 데이터를 upload한 상태이지만, 최종 결과가 도착하지 않은 상태.
				// 결과가 도착할 때까지 대기함
				s_logger.trace("EOS & wait for the result");
				awaitResultInGuard();
			}
			
			switch ( m_state ) {
				case UPLOADING:
					// 데이터를 계속 올리는 중이지만, server쪽에서 consumer가 성공적으로
					// 종료하고 결과를 보내온 상태
					m_channel.onNext(PBUtils.EMPTY_UP_MESSAGE);
					m_channel.onCompleted();			
					s_logger.debug("finished (before EOS): result=" + m_result);
					return m_result;
				case END_OF_STREAM:
					// 모든 데이터를 upload한 상태에서 server쪽에서 consumer가 성공적으로
					// 종료하고 결과를 보내온 상태
					m_channel.onNext(PBUtils.EMPTY_UP_MESSAGE);
					m_channel.onCompleted();
					s_logger.debug("finished: result=" + m_result);
					return m_result;
				case CANCELLED:
					throw new CancellationException();
				case FAILED:
					throw m_cause;
				default: throw new AssertionError();
			}
		}
		finally {
			m_guard.unlock();
		}
	}

	@Override
	public void onNext(DownMessage resp) {
		switch ( resp.getEitherCase() ) {
			case RESULT:
				// peer로부터 upload 결과가 도착한 경우.
				ByteString result = resp.getResult();
				s_logger.trace("received RESULT: {}", result);
				m_guard.run(() -> m_result = result);
				break;
			case OFFSET:
				m_tail = resp.getOffset();
				String gapStr = UnitUtils.toByteSizeString(m_head-m_tail);
				System.out.printf("%d - %d = %s%n", m_head, m_tail, gapStr);
			case ERROR:
				handleRemoteException(PBUtils.toException(resp.getError()));
				break;
			case DUMMY: break;
			default: throw new AssertionError();
		}
	}

	@Override
	public void onError(Throwable cause) {
		handleRemoteException(cause);
	}

	@Override
	public void onCompleted() {
		handleRemoteException(new CancellationException());
	}
	
	@Override
	public String toString() {
		return String.format("%s[%s, result=%s]", getClass().getSimpleName(), m_state, ""+m_result);
	}
	
	private void handleRemoteException(Throwable cause) {
		m_guard.run(() -> {
			if ( m_state == State.CANCELLED || m_state == State.FAILED || m_result != null ) {
				return;
			}
			if ( cause instanceof CancellationException ) {
				s_logger.info("peer cancels the operation");
				m_state = State.CANCELLED;
			}
			else {
				s_logger.warn("received ERROR[cause=" + cause + "]");
				m_state = State.FAILED;
				m_cause = Throwables.toException(cause);
			}
			
			m_channel.onNext(PBUtils.EMPTY_UP_MESSAGE);
			m_channel.onCompleted();
		});
	}
	
	private void awaitResultInGuard() {
		Date due = new Date(System.currentTimeMillis() + DEFAULT_CLOSE_TIMEOUT);
		try {
			while ( m_result == null && !(m_state == State.CANCELLED || m_state == State.FAILED) ) {
				if ( !m_guard.awaitSignal(due) ) {
					throw new TimeoutException();
				}
			}
		}
		catch ( InterruptedException e ) {
			m_channel.onNext(UpMessage.newBuilder()
									.setError(PBUtils.ERROR(Code.CANCELLED, "user interruption"))
									.build());
			m_channel.onCompleted();
			m_cause = new CancellationException();
			
			m_state = State.CANCELLED;
			m_guard.signalAll();
		}
		catch ( Exception e ) {
			m_channel.onNext(UpMessage.newBuilder().setError(PBUtils.ERROR(e)).build());
			m_channel.onCompleted();
			m_cause = e;
			
			m_state = State.FAILED;
			m_guard.signalAll();
		}
	}
}
