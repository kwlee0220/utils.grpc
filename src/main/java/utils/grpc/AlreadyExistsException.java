package utils.grpc;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AlreadyExistsException extends RuntimeException {
	private static final long serialVersionUID = 1L;

	public AlreadyExistsException(String details) {
		super(details);
	}
}
