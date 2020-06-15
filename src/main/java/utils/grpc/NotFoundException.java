package utils.grpc;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class NotFoundException extends RuntimeException {
	private static final long serialVersionUID = 1L;

	public NotFoundException(String details) {
		super(details);
	}
}
