package utils.grpc.stream;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.GuardedBy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import utils.Throwables;
import utils.UnitUtils;
import utils.async.Guard;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SuppliableInputStream extends InputStream {
	private static final Logger s_logger = LoggerFactory.getLogger(SuppliableInputStream.class);
	
	private final int m_maxQueueLength;
	
	private final Guard m_guard = Guard.create();
	@GuardedBy("m_guard") private final List<ByteBuffer> m_chunkQueue;
	@GuardedBy("m_guard") private int m_current = 0;
	@GuardedBy("m_guard") private boolean m_closed = false;
	@GuardedBy("m_guard") private boolean m_eos = false;
	@GuardedBy("m_guard") private Throwable m_cause = null;
	@GuardedBy("m_guard") private ByteBuffer m_buffer;
	private long m_totalOffset = 0;
	
	public static SuppliableInputStream create() {
		return new SuppliableInputStream();
	}
	
	public static SuppliableInputStream create(int chunkQLength) {
		return new SuppliableInputStream(chunkQLength);
	}
	
	private SuppliableInputStream(int chunkQLength) {
		m_maxQueueLength = chunkQLength;
		m_chunkQueue = Lists.newArrayListWithCapacity(chunkQLength);
		m_buffer = null;
	}
	
	private SuppliableInputStream() {
		m_maxQueueLength = Integer.MAX_VALUE;
		m_chunkQueue = Lists.newArrayList();
		m_buffer = null;
	}
	
	@Override
	public void close() throws IOException {
		m_guard.lock();
		try {
			m_chunkQueue.clear();
			m_closed = true;
			
			m_guard.signalAll();
		}
		finally {
			m_guard.unlock();
		}
		
		super.close();
	}
	
	public boolean isClosed() {
		return m_guard.get(() -> m_closed);
	}
	
	public int getMaxQueueLength() {
		return m_maxQueueLength;
	}
	
	public int getQueueLength() {
		return m_guard.get(() -> m_chunkQueue.size());
	}
	
	public Guard getGuard() {
		return m_guard;
	}
	
	public long offset() {
		return m_totalOffset;
	}
	
	public int getCurrentChunkSeqNo() {
		return m_guard.get(() -> m_current);
	}
	
	public boolean awaitActiveChunk(int chunkNo, long timeout, TimeUnit tu)
		throws InterruptedException {
		Date due = new Date(System.currentTimeMillis() + tu.toMillis(timeout));
		
		m_guard.lock();
		try {
			while ( !isClosed() && m_current < chunkNo ) {
				if ( !m_guard.awaitUntil(due) ) {
					return false;
				}
			}
			
			return true;
		}
		finally {
			m_guard.unlock();
		}
	}

	@Override
	public int read() throws IOException {
		ByteBuffer chunk = locateCurrentChunk();
		if ( chunk != null ) {
			++m_totalOffset;
			return chunk.get() & 0x000000ff;
		}
		else {
			return -1;
		}
	}

	@Override
    public int read(byte b[], int off, int len) throws IOException {
		ByteBuffer chunk = locateCurrentChunk();
		if ( chunk != null ) {
			int nbytes = Math.min(chunk.remaining(), len);
			
			chunk.get(b, off, nbytes);
			m_totalOffset += nbytes;

			return nbytes;
		}
		else {
			return -1;
		}
    }
	
	public void supply(ByteBuffer chunk) throws InterruptedException, StreamClosedException {
		m_guard.lock();
		try {
			while ( true ) {
				if ( m_closed ) {
					throw new StreamClosedException("Stream is closed already");
				}
				if ( m_eos ) {
					return;
				}
				if ( m_chunkQueue.size() < m_maxQueueLength ) {
					m_chunkQueue.add(chunk);
					m_guard.signalAll();
					
					return;
				}
				
				m_guard.await();
			}
		}
		finally {
			m_guard.unlock();
		}
	}
	
	/**
	 * 주어진 데이터를 입력 스트림에 추가시킨다.
	 * 
	 * @param chunk		제공할 데이터 chunk
	 * @param timeout	시간제한 시간
	 * 					0이면 바로 {@link TimeoutException} 발생,
	 * 					양수이면 지정된 시간만큼만 대기 후 {@link TimeoutException} 발생,
	 * 					음수이면 무한 대기
	 * @param unit		제한 시간 단위
	 * @throws InterruptedException	데이터 추가 과정에서 대기가 발생한 상태에서 대기 쓰레드가
	 * 					중단된 경우.
	 * @throws StreamClosedException	입력 스트림이 닫힌 경우
	 * @throws TimeoutException		데이터 추가 과정에서 대기가 발생한 상태에서
	 * 								지정된 시간이 경과한 경우.
	 */
	public void supply(ByteBuffer chunk, long timeout, TimeUnit unit)
		throws InterruptedException, StreamClosedException, TimeoutException {
		Date due = (timeout > 0) ? new Date(System.currentTimeMillis() + unit.toMillis(timeout)) : null;
		
		m_guard.lock();
		try {
			while ( true ) {
				if ( m_closed ) {
					throw new StreamClosedException("Stream is closed already");
				}
				if ( m_eos ) {
					return;
				}
				if ( m_chunkQueue.size() < m_maxQueueLength ) {
					m_chunkQueue.add(chunk);
					m_guard.signalAll();
					
					return;
				}
				
				if ( timeout < 0 ) {
					m_guard.await();
				}
				else if ( timeout == 0 ) {
					throw new TimeoutException("supply timeout");
				}
				else {
					if ( !m_guard.awaitUntil(due) ) {
						String details = String.format("supply timeout: %s",
														UnitUtils.toMillisString(unit.toMillis(timeout)));
						throw new TimeoutException(details);
					}
				}
			}
		}
		finally {
			m_guard.unlock();
		}
	}
	
	public void endOfSupply() {
		m_guard.runAndSignalAll(() -> m_eos = true);
	}
	
	public void endOfSupply(Throwable cause) {
		m_guard.runAndSignalAll(() -> {
			if ( !m_eos ) {
				m_eos = true;
				m_cause = cause;
			}
		});
	}
	
	@Override
	public String toString() {
		String chunkStr = String.format("[%d:%d]", m_current, m_chunkQueue.size());
		return String.format("%s[chunks%s, %s, %s, offset=%d]",
								getClass().getSimpleName(), chunkStr,
								m_closed ? "closed" : "open",
								m_eos ? "eos" : "supplying",
								m_totalOffset);
	}
	
	private ByteBuffer locateCurrentChunk() throws IOException {
		m_guard.lock();
		try {
			if ( m_closed ) {
				throw new IOException("closed already");
			}
			
			if ( m_buffer == null || !m_buffer.hasRemaining() ) {
				m_buffer = getNextChunkInGuard();
			}
			
			return m_buffer;
		}
		catch ( InterruptedException e ) {
			throw new IOException("" + e);
		}
		finally {
			m_guard.unlock();
		}
	}
	
	private ByteBuffer getNextChunkInGuard() throws InterruptedException, IOException {
		if ( !m_chunkQueue.isEmpty() && m_current > 0 ) {
			m_chunkQueue.remove(0);
		}
		
		while ( m_chunkQueue.isEmpty() ) {
			if ( m_closed ) {
				throw new IOException("Stream has been closed already");
			}
			if ( m_eos ) {
				// 더 이상의  chunk supply가 없는 경우, 정상적으로 종료된 것인지
				// 오류에 의한 종료인지를 확인한다.
				if ( m_cause != null ) {
					Throwables.throwIfInstanceOf(m_cause, IOException.class);
					Throwables.sneakyThrow(m_cause);
				}
				return null;
			}
			
			m_guard.await();
		}

		ByteBuffer head = m_chunkQueue.get(0);
		++m_current;
		s_logger.debug("get_next_chunk: {}", this);
		m_guard.signalAll();
		
		return head;
	}
}