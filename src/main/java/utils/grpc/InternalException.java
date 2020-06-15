package utils.grpc;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class InternalException extends RuntimeException {
	private static final long serialVersionUID = 1L;

	public InternalException(String details) {
		super(details);
	}
}
