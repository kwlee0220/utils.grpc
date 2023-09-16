package utils.grpc.stream.client;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Date;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.GuardedBy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import io.grpc.stub.StreamObserver;
import proto.ErrorProto.Code;
import proto.stream.DownMessage;
import proto.stream.UpMessage;
import utils.Throwables;
import utils.UnitUtils;
import utils.async.Guard;
import utils.func.FOption;
import utils.grpc.PBUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class StreamUploadOutputStream extends OutputStream implements StreamObserver<DownMessage> {
	private static final Logger s_logger = LoggerFactory.getLogger(StreamUploadOutputStream.class);
	private static final long DEFAULT_CLOSE_TIMEOUT = UnitUtils.parseDurationMillis("30s");		// 30s

	private final ByteString m_header;
	private StreamObserver<UpMessage> m_channel;
	
	private final Guard m_guard = Guard.create();
	@GuardedBy("m_guard") private State m_state = State.NOT_STARTED;
	@GuardedBy("m_guard") private ByteString m_result = null;
	@GuardedBy("m_guard") private Exception m_cause = null;
	@GuardedBy("m_guard") private int m_chunkCount = 0;
	
	private static enum State {
		NOT_STARTED,
		UPLOADING,
		END_OF_STREAM,
		CANCELLED,
		FAILED,
	}
	
	public StreamUploadOutputStream(ByteString header) {
		m_header = header;
	}
	
	public void setOutgoingChannel(StreamObserver<UpMessage> channel) {
		m_channel = channel;
		
		m_guard.runAndSignalAll(() -> {
			s_logger.trace("send HEADER: {}", m_header);
			UpMessage req = UpMessage.newBuilder().setHeader(m_header).build();
			m_channel.onNext(req);
			
			m_state = State.UPLOADING;
		});
	}

	@Override
	public void write(int b) throws IOException {
		throw new IOException("unsupported operation");
	}
	
	@Override
    public void write(byte b[]) throws IOException {
		write(b, 0, b.length);
	}
	
	private static final int CHUNK_SIZE = (int)UnitUtils.parseByteSize("256kb");

	@Override
    public void write(byte bytes[], int offset, int size) throws IOException {
		int remains = size;
		while ( remains > 0 ) {
			int chunkSize = Math.min(remains, CHUNK_SIZE);
			
			writeChunk(bytes, offset, chunkSize);
			offset += chunkSize;
			remains -= chunkSize;
		}
    }

	@Override
    public void close() throws IOException {
		UpMessage eos = UpMessage.newBuilder().setEos(PBUtils.VOID()).build();
		
		m_guard.lock();
		try {
			if ( m_state == State.UPLOADING ) {
				// 서버에게 모든 데이터를 전송했다는 것을 알린다.
				s_logger.trace("send END_OF_STREAM");
				
				m_channel.onNext(eos);
				m_state = State.END_OF_STREAM;
			}
			else {
				throw new IllegalStateException("expected=" + State.UPLOADING + ", but=" + m_state);
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
				m_guard.runAndSignalAll(() -> m_result = result);
				break;
			case ERROR:
				s_logger.info("received a failure (thru onNext): " + resp.getError());
				handleRemoteException(PBUtils.toException(resp.getError()));
				break;
			case DUMMY: break;
			default: throw new AssertionError();
		}
	}

	@Override
	public void onError(Throwable cause) {
		s_logger.info("received a failure (thru onError): " + cause);
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
		m_guard.runAndSignalAll(() -> {
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
	
	public FOption<ByteString> pollResult() throws Exception {
		m_guard.lock();
		try {
			if ( m_result != null ) {
				return FOption.of(m_result);
			}
			switch ( m_state ) {
				case END_OF_STREAM:
					return FOption.empty();
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
	
	public ByteString awaitResult() throws Exception {
		m_guard.lock();
		try {
			while ( true ) {
				if ( m_result != null ) {
					return m_result;
				}
				switch ( m_state ) {
					case END_OF_STREAM:
					case UPLOADING:
						break;
					case CANCELLED:
						throw new CancellationException();
					case FAILED:
						throw m_cause;
					default: throw new AssertionError();
				}
				
				m_guard.await();
			}
		}
		finally {
			m_guard.unlock();
		}
	}
	
	private void awaitResultInGuard() {
		Date due = new Date(System.currentTimeMillis() + DEFAULT_CLOSE_TIMEOUT);
		try {
			while ( m_result == null && !(m_state == State.CANCELLED || m_state == State.FAILED) ) {
				if ( !m_guard.awaitUntil(due) ) {
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

    private void writeChunk(byte bytes[], int offset, int size) throws IOException {
		ByteString chunk = ByteString.copyFrom(bytes, offset, size);
		
		m_guard.lock();
		try {
			if ( m_result != null ) {
				// 서버측에서 결과를 이미 보내온 상태
				// 입력 데이터를 무시한다.
				return;
			}
			
			switch ( m_state ) {
				case NOT_STARTED:
					throw new IllegalStateException("not started");
				case UPLOADING:	// 정상적인 상태
					++m_chunkCount;
					s_logger.trace("send CHUNK[idx={}, size={}]", m_chunkCount, chunk.size());
					UpMessage block = UpMessage.newBuilder().setBlock(chunk).build();
					m_channel.onNext(block);
					break;
				case CANCELLED:	// 서버측에서 전송받는 것을 취소한 상태
					throw new IOException("receiver has cancelled the upload");
				case FAILED:	// 서버측에서 예외가 발생한 상태
					throw new IOException("an exception has been raised at the server", m_cause);
				default: throw new AssertionError();
			}
		}
		finally {
			m_guard.unlock();
		}
    }
}
