package utils.grpc.stream;

import java.io.IOException;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class StreamClosedException extends IOException {
	private static final long serialVersionUID = -3806426243923855218L;

	public StreamClosedException(String deatils) {
		super(deatils);
	}
}
