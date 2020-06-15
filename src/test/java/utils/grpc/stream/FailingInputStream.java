package utils.grpc.stream;

import java.io.IOException;
import java.io.InputStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FailingInputStream  extends InputStream {
	private final InputStream m_is;
	private final IOException m_error;
	private int m_remains;
	
	FailingInputStream(InputStream is, int length, IOException cause) {
		m_is = is;
		m_remains = length;
		m_error = cause;
	}

	@Override
	public int read() throws IOException {
		if ( --m_remains >= 0 ) {
			return m_is.read();
		}
		else {
			throw m_error;
		}
	}
	
	@Override
    public int read(byte b[], int off, int len) throws IOException {
		if ( m_remains > 0 ) {
			len = Math.min(len, m_remains);
			int nbytes = m_is.read(b, off, len);
			m_remains -= nbytes;
			
			return nbytes;
		}
		else {
			throw m_error;
		}
	}
}
