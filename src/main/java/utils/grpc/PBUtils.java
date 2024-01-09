package utils.grpc;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeoutException;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import proto.BoolProto;
import proto.BoolResponse;
import proto.ErrorProto;
import proto.ErrorProto.Code;
import proto.Int64Proto;
import proto.StringProto;
import proto.StringResponse;
import proto.VoidProto;
import proto.VoidResponse;
import proto.stream.DownMessage;
import proto.stream.UpMessage;
import utils.CSV;
import utils.Throwables;
import utils.func.CheckedRunnable;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PBUtils {
	private static VoidProto s_void = null;
	public static VoidProto VOID() {
		if ( s_void == null ) {
			s_void = VoidProto.newBuilder().build();
		}
		
		return s_void;
	}
	
	private static VoidResponse s_voidResponse = null;
	public static VoidResponse VOID_RESPONSE() {
		if ( s_voidResponse == null ) {
			s_voidResponse = VoidResponse.newBuilder().setValue(VOID()).build();
		}
		return s_voidResponse;
	}
	public static VoidResponse VOID_RESPONSE(ErrorProto error) {
		return VoidResponse.newBuilder().setError(error).build();
	}
	public static VoidResponse VOID_RESPONSE(Throwable e) {
		return VoidResponse.newBuilder().setError(ERROR(e)).build();
	}
	public static void handle(VoidResponse resp) {
		switch ( resp.getEitherCase() ) {
			case VALUE:
				return;
			case ERROR:
				throw Throwables.toRuntimeException(toException(resp.getError()));
			default:
				throw new AssertionError();
		}
	}
	
	public static StringProto STRING(String str) {
		return StringProto.newBuilder().setValue(str).build();
	}
	public static String STRING(ByteString bstr) throws InvalidProtocolBufferException {
		return StringProto.parseFrom(bstr).getValue();
	}
	public static StringResponse STRING_RESPONSE(String value) {
		return StringResponse.newBuilder()
							.setValue(STRING(value))
							.build();
	}
	public static StringResponse STRING_RESPONSE(Throwable e) {
		return StringResponse.newBuilder()
							.setError(ERROR(e))
							.build();
	}
	public static String handle(StringResponse resp) {
		switch ( resp.getEitherCase() ) {
			case VALUE:
				return resp.getValue().getValue();
			case ERROR:
				throw Throwables.toRuntimeException(toException(resp.getError()));
			default:
				throw new AssertionError();
		}
	}
	
	public static BoolProto BOOL(boolean value) {
		return BoolProto.newBuilder().setValue(value).build();
	}
	public static BoolResponse BOOL_RESPONSE(boolean value) {
		return BoolResponse.newBuilder()
							.setValue(BOOL(value))
							.build();
	}
	public static BoolResponse BOOL_RESPONSE(Throwable e) {
		return BoolResponse.newBuilder()
							.setError(ERROR(e))
							.build();
	}
	public static boolean handle(BoolResponse resp) {
		switch ( resp.getEitherCase() ) {
			case VALUE:
				return resp.getValue().getValue();
			case ERROR:
				throw Throwables.toRuntimeException(toException(resp.getError()));
			default:
				throw new AssertionError();
		}
	}
	
	public static ByteString BYTE_STRING(String str) {
		return StringProto.newBuilder().setValue(str).build().toByteString();
	}
	public static ByteString BYTE_STRING(long value) {
		return Int64Proto.newBuilder().setValue(value).build().toByteString();
	}
	
	public static Int64Proto INT64(long value) {
		return Int64Proto.newBuilder().setValue(value).build();
	}
	
	public static ErrorProto ERROR(Code code, String details) {
		if ( details == null ) {
			details = "";
		}
		
		return ErrorProto.newBuilder().setCode(code).setDetails(details).build();
	}
	
	public static ErrorProto ERROR(Throwable e) {
		if ( e instanceof CancellationException ) {
			return ERROR(Code.CANCELLED, e.getMessage());
		}
		else if ( e instanceof IllegalArgumentException ) {
			return ERROR(Code.INVALID_ARGUMENT, e.getMessage());
		}
		else if ( e instanceof IllegalStateException ) {
			return ERROR(Code.INVALID_STATE, e.getMessage());
		}
		else if ( e instanceof NotFoundException ) {
			return ERROR(Code.NOT_FOUND, e.getMessage());
		}
		else if ( e instanceof AlreadyExistsException ) {
			return ERROR(Code.ALREADY_EXISTS, e.getMessage());
		}
		else if ( e instanceof TimeoutException ) {
			return ERROR(Code.TIMEOUT, e.getMessage());
		}
		else if ( e instanceof IOException || e instanceof UncheckedIOException ) {
			return ERROR(Code.IO_ERROR, e.getMessage());
		}
		else if ( e instanceof StatusException ) {
			Status status = ((StatusException)e).getStatus();
			return ERROR(Code.GRPC_STATUS, status.getCode().name() + ":" + status.getDescription());
		}
		else if ( e instanceof StatusRuntimeException ) {
			Status status = ((StatusRuntimeException)e).getStatus();
			return ERROR(Code.GRPC_STATUS, status.getCode().name() + ":" + status.getDescription());
		}
		else if ( e instanceof InternalException ) {
			return ERROR(Code.INTERNAL, e.getMessage());
		}
		else {
			return ERROR(Code.INTERNAL, e.getMessage());
		}
	}
	
	public static Exception toException(ErrorProto error) {
		switch ( error.getCode() ) {
			case CANCELLED:
				return new CancellationException(error.getDetails());
			case INVALID_ARGUMENT:
				return new IllegalArgumentException(error.getDetails());
			case INVALID_STATE:
				return new IllegalStateException(error.getDetails());
			case NOT_FOUND:
				return new NotFoundException(error.getDetails());
			case ALREADY_EXISTS:
				return new AlreadyExistsException(error.getDetails());
			case TIMEOUT:
				return new TimeoutException(error.getDetails());
			case IO_ERROR:
				return new IOException(error.getDetails());
			case GRPC_STATUS:
				String[] parts = CSV.parseCsv(error.getDetails(), ':').toArray(String.class);
				Status.Code code = Status.Code.valueOf(parts[0]);
				String desc = parts[1];
				return Status.fromCode(code).withDescription(desc).asRuntimeException();
			case INTERNAL:
				return new InternalException(error.getDetails());
			default:
				return new RuntimeException(error.getDetails());
		}
	}

	public static boolean isCancelled(ErrorProto error) {
		return error.getCode() == ErrorProto.Code.CANCELLED;
	}
	
	public static void replyVoid(CheckedRunnable runnable, StreamObserver<VoidResponse> response) {
		try {
			runnable.run();
			response.onNext(VOID_RESPONSE());
		}
		catch ( Throwable e ) {
			response.onNext(VOID_RESPONSE(e));
		}
		response.onCompleted();
	}
	
//	public static Status getStatus(Throwable e) {
//		if ( e instanceof StatusRuntimeException ) {
//			return ((StatusRuntimeException)e).getStatus();
//		}
//		else if ( e instanceof StatusException ) {
//			return ((StatusException)e).getStatus();
//		}
//		throw new AssertionError("not StatusException: " + e);
//	}
//
//	public static boolean isOperationCancelled(Throwable cause) {
//		Status status = getStatus(cause);
//		return status.getCode() == Status.CANCELLED.getCode();
//	}
//	
//	public static StatusRuntimeException toStatusRuntimeException(Throwable cause) {
//		if ( cause instanceof StatusRuntimeException ) {
//			return (StatusRuntimeException)cause;
//		}
//		else if ( cause instanceof StatusException ) {
//			return ((StatusException)cause).getStatus().asRuntimeException();
//		}
//		else {
//			return INTERNAL_ERROR(cause);
//		}
//	}
//	
//	public static StatusRuntimeException CANCELLED() {
//		return Status.CANCELLED.withDescription("operation cancelled").asRuntimeException();
//	}
//	
//	public static StatusRuntimeException INTERNAL_ERROR(Throwable cause) {
//		return Status.INTERNAL.withDescription("" + cause).asRuntimeException();
//	}
//	
//	public static StatusRuntimeException INTERNAL_ERROR(String desc) {
//		return Status.INTERNAL.withDescription(desc).asRuntimeException();
//	}
//	
//	public static DownMessage toResultDownMessage(ByteString chunk) {
//		return DownMessage.newBuilder().setResult(chunk).build();
//	}
//	public static DownMessage toErrorDownMessage(Throwable error) {
//		return DownMessage.newBuilder().setError(ERROR(error)).build();
//	}
	
	public static final DownMessage EMPTY_DOWN_MESSAGE = DownMessage.newBuilder()
																	.setDummy(VOID())
																	.build();
	public static final UpMessage EMPTY_UP_MESSAGE = UpMessage.newBuilder()
																.setDummy(VOID())
																.build();
	
/*
	public static final SerializedProto serialize(Object obj) {
		if ( obj instanceof PBSerializable ) {
			return ((PBSerializable<?>)obj).serialize();
		}
		else if ( obj instanceof Message ) {
			return PBUtils.serialize((Message)obj);
		}
		else if ( obj instanceof Serializable ) {
			return PBUtils.serializeJava((Serializable)obj);
		}
		else {
			throw new IllegalStateException("unable to serialize: " + obj);
		}
	}
	
	public static final SerializedProto serializeJava(Serializable obj) {
		try {
			JavaSerializedProto proto = JavaSerializedProto.newBuilder()
										.setSerialized(ByteString.copyFrom(IOUtils.serialize(obj)))
										.build();
			return SerializedProto.newBuilder()
									.setJava(proto)
									.build();
		}
		catch ( Exception e ) {
			throw new PBException("fails to serialize object: proto=" + obj, e);
		}
	}
	
	public static final SerializedProto serialize(Message proto) {
		ProtoBufSerializedProto serialized = ProtoBufSerializedProto.newBuilder()
										.setProtoClass(proto.getClass().getName())
										.setSerialized(proto.toByteString())
										.build();
		return SerializedProto.newBuilder()
								.setProtoBuf(serialized)
								.build();
	}
	
	public static final <T> T deserialize(SerializedProto proto) {
		switch ( proto.getMethodCase() ) {
			case PROTO_BUF:
				return deserialize(proto.getProtoBuf());
			case JAVA:
				return deserialize(proto.getJava());
			default:
				throw new AssertionError("unregistered serialization method: method="
										+ proto.getMethodCase());
		}
	}
	
	@SuppressWarnings("unchecked")
	public static <T> T deserialize(JavaSerializedProto proto) {
		try {
			return (T)IOUtils.deserialize(proto.getSerialized().toByteArray());
		}
		catch ( Exception e ) {
			Throwable cause = Throwables.unwrapThrowable(e);
			throw new PBException("fails to deserialize: proto=" + proto, cause);
		}
	}
	
	@SuppressWarnings("unchecked")
	public static final <T> T deserialize(ProtoBufSerializedProto proto) {
		try {
			ByteString serialized = proto.getSerialized();
			
			FOption<String> clsName = getOptionField(proto, "object_class");
			Class<?> protoCls = Class.forName(proto.getProtoClass());

			Method parseFrom = protoCls.getMethod("parseFrom", ByteString.class);
			Message optorProto = (Message)parseFrom.invoke(null, serialized);
			
			if ( clsName.isPresent() ) {
				Class<?> cls = Class.forName(clsName.get());
				Method fromProto = cls.getMethod("fromProto", protoCls);
				return (T)fromProto.invoke(null, optorProto);
			}
			else {
				return (T)ProtoBufActivator.activate(optorProto);
			}
		}
		catch ( Exception e ) {
			Throwable cause = Throwables.unwrapThrowable(e);
			throw new PBException("fails to deserialize: proto=" + proto + ", cause=" + cause, cause);
		}
	}
	
	public static Enum<?> getCase(Message proto, String field) {
		try {
			String partName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, field);
			Method getCase = proto.getClass().getMethod("get" + partName + "Case", new Class<?>[0]);
			return (Enum<?>)getCase.invoke(proto, new Object[0]);
		}
		catch ( Exception e ) {
			throw new PBException("fails to get the case " + field, e);
		}
	}

	@SuppressWarnings("unchecked")
	public static <T> FOption<T> getOptionField(Message proto, String field) {
		try {
			return FOption.ofNullable((T)KVFStream.from(proto.getAllFields())
												.filter(kv -> kv.key().getName().equals(field))
												.next()
												.map(kv -> kv.value())
												.getOrNull());
					}
		catch ( Exception e ) {
			throw new PBException("fails to get the field " + field, e);
		}
	}

	public static FOption<String> getStringOptionField(Message proto, String field) {
		return getOptionField(proto, field).cast(String.class);
	}

	public static FOption<Double> getDoubleOptionField(Message proto, String field) {
		return getOptionField(proto, field).cast(Double.class);
	}

	public static FOption<Long> geLongOptionField(Message proto, String field) {
		return getOptionField(proto, field).cast(Long.class);
	}

	public static FOption<Integer> getIntOptionField(Message proto, String field) {
		return getOptionField(proto, field).cast(Integer.class);
	}

	@SuppressWarnings("unchecked")
	public static <T> T getField(Message proto, String field) {
		try {
			return(T)KVFStream.from(proto.getAllFields())
									.filter(kv -> kv.key().getName().equals(field))
									.next()
									.map(kv -> kv.value())
									.getOrElseThrow(()
										-> new PBException("unknown field: name=" + field
																	+ ", msg=" + proto));
		}
		catch ( Exception e ) {
			throw new PBException("fails to get the field " + field, e);
		}
	}
*/

/*
	public static String getValue(StringResponse resp) {
		switch ( resp.getEitherCase() ) {
			case VALUE:
				return resp.getValue();
			case ERROR:
				throw Throwables.toRuntimeException(PBUtils.toException(resp.getError()));
			default:
				throw new AssertionError();
		}
	}
	
	public static BoolResponse toBoolResponse(boolean value) {
		return BoolResponse.newBuilder()
							.setValue(value)
							.build();
	}
	public static BoolResponse toBoolResponse(Throwable e) {
		return BoolResponse.newBuilder()
							.setError(toErrorProto(e))
							.build();
	}
	public static LongResponse toLongResponse(Throwable e) {
		return LongResponse.newBuilder()
							.setError(toErrorProto(e))
							.build();
	}
	
	public static FloatResponse toFloatResponse(float value) {
		return FloatResponse.newBuilder()
							.setValue(value)
							.build();
	}
	
	public static FloatResponse toFloatResponse(Throwable e) {
		return FloatResponse.newBuilder()
							.setError(toErrorProto(e))
							.build();
	}
	
	public static DoubleResponse toDoubleResponse(double value) {
		return DoubleResponse.newBuilder()
							.setValue(value)
							.build();
	}
	
	public static DoubleResponse toDoubleResponse(Throwable e) {
		return DoubleResponse.newBuilder()
							.setError(toErrorProto(e))
							.build();
	}
	
	public static RecordResponse toRecordResponse(Record value) {
		return RecordResponse.newBuilder()
							.setRecord(value.toProto())
							.build();
	}
	
	public static RecordResponse toRecordResponse(Throwable e) {
		return RecordResponse.newBuilder()
							.setError(toErrorProto(e))
							.build();
	}

	public static <T extends Message> FStream<T> toFStream(Iterator<T> respIter) {
		if ( !respIter.hasNext() ) {
			return FStream.empty();
		}
		
		PeekingIterator<T> piter = Iterators.peekingIterator(respIter);
		T proto = piter.peek();
		FOption<ErrorProto> error = getOptionField(proto, "error");
		if ( error.isPresent() ) {
			throw Throwables.toRuntimeException(toException(error.get()));
		}
		
		return FStream.from(piter);
	}
	
	public static void handle(VoidResponse resp) {
		switch ( resp.getEitherCase() ) {
			case VOID:
				return;
			case ERROR:
				throw Throwables.toRuntimeException(toException(resp.getError()));
			default:
				throw new AssertionError();
		}
	}
	
	public static String handle(StringResponse resp) {
		switch ( resp.getEitherCase() ) {
			case VALUE:
				return resp.getValue();
			case ERROR:
				throw Throwables.toRuntimeException(toException(resp.getError()));
			default:
				throw new AssertionError();
		}
	}
	
	public static boolean handle(BoolResponse resp) {
		switch ( resp.getEitherCase() ) {
			case VALUE:
				return resp.getValue();
			case ERROR:
				throw Throwables.toRuntimeException(toException(resp.getError()));
			default:
				throw new AssertionError();
		}
	}
	
	public static long handle(LongResponse resp) {
		switch ( resp.getEitherCase() ) {
			case VALUE:
				return resp.getValue();
			case ERROR:
				throw Throwables.toRuntimeException(toException(resp.getError()));
			default:
				throw new AssertionError();
		}
	}
	
	public static float handle(FloatResponse resp) {
		switch ( resp.getEitherCase() ) {
			case VALUE:
				return resp.getValue();
			case ERROR:
				throw Throwables.toRuntimeException(toException(resp.getError()));
			default:
				throw new AssertionError();
		}
	}
	
	public static double handle(DoubleResponse resp) {
		switch ( resp.getEitherCase() ) {
			case VALUE:
				return resp.getValue();
			case ERROR:
				throw Throwables.toRuntimeException(toException(resp.getError()));
			default:
				throw new AssertionError();
		}
	}
	
	public static <X extends Throwable> void replyBoolean(CheckedSupplier<Boolean> supplier,
									StreamObserver<BoolResponse> response) {
		try {
			boolean done = supplier.get();
			response.onNext(BoolResponse.newBuilder()
										.setValue(done)
										.build());
		}
		catch ( Throwable e ) {
			response.onNext(BoolResponse.newBuilder()
										.setError(PBUtils.toErrorProto(e))
										.build());
		}
		response.onCompleted();
	}
	
	public static <X extends Throwable> void replyLong(CheckedSupplier<Long> supplier,
							StreamObserver<LongResponse> response) {
		try {
			long ret = supplier.get();
			response.onNext(LongResponse.newBuilder()
										.setValue(ret)
										.build());
		}
		catch ( Throwable e ) {
			response.onNext(LongResponse.newBuilder()
										.setError(PBUtils.toErrorProto(e))
										.build());
		}
		response.onCompleted();
	}
	
	public static <X extends Throwable> void replyString(CheckedSupplier<String> supplier,
									StreamObserver<StringResponse> response) {
		try {
			String ret = supplier.get();
			response.onNext(StringResponse.newBuilder()
										.setValue(ret)
										.build());
		}
		catch ( Throwable e ) {
			response.onNext(StringResponse.newBuilder()
										.setError(PBUtils.toErrorProto(e))
										.build());
		}
		response.onCompleted();
	}
	
	public static void replyVoid(CheckedRunnable runnable,
									StreamObserver<VoidResponse> response) {
		try {
			runnable.run();
			response.onNext(VoidResponse.newBuilder()
										.setVoid(VOID)
										.build());
		}
		catch ( Throwable e ) {
			response.onNext(VoidResponse.newBuilder()
										.setError(PBUtils.toErrorProto(e))
										.build());
		}
		response.onCompleted();
	}
	
	public static byte[] toDelimitedBytes(Message proto) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			proto.writeTo(baos);
		}
		catch ( IOException e ) {
			throw new RuntimeException(e);
		}
		finally {
			IOUtils.closeQuietly(baos);
		}
		
		return baos.toByteArray();
	}

	public static boolean fromProto(BoolProto proto) {
		return proto.getValue();
	}
	
	public static BoolProto toProto(boolean value) {
		return BoolProto.newBuilder().setValue(value).build();
	}
	
	public static Size2i fromProto(Size2iProto proto) {
		return new Size2i(proto.getWidth(), proto.getHeight());
	}
	
	public static Size2iProto toProto(Size2i dim) {
		return Size2iProto.newBuilder()
									.setWidth(dim.getWidth())
									.setHeight(dim.getHeight())
									.build();
	}
	
	public static Size2d fromProto(Size2dProto proto) {
		return new Size2d(proto.getWidth(), proto.getHeight());
	}
	
	public static Size2dProto toProto(Size2d dim) {
		return Size2dProto.newBuilder()
						.setWidth(dim.getWidth())
						.setHeight(dim.getHeight())
						.build();
	}
	
	public static Interval fromProto(IntervalProto proto) {
		return Interval.between(proto.getStart(), proto.getEnd());
	}
	
	public static IntervalProto toProto(Interval intvl) {
		return IntervalProto.newBuilder()
							.setStart(intvl.getStartMillis())
							.setEnd(intvl.getEndMillis())
							.build();
	}
	
	public static Coordinate fromProto(CoordinateProto proto) {
		return new Coordinate(proto.getX(), proto.getY());
	}
	
	public static CoordinateProto toProto(Coordinate coord) {
		return CoordinateProto.newBuilder()
								.setX(coord.x)
								.setY(coord.y)
								.build();
	}
	
	public static Point fromProto(PointProto proto) {
		double x = proto.getX();
		if ( Double.isNaN(x) ) {
			return GeoClientUtils.EMPTY_POINT;
		}
		else {
			return GeoClientUtils.toPoint(x, proto.getY());
		}
	}
	
	private static final PointProto EMPTY_POINT
					= PointProto.newBuilder().setX(Double.NaN).setY(Double.NaN).build();
	public static PointProto toProto(Point pt) {
		return pt.isEmpty()
				? EMPTY_POINT
				: PointProto.newBuilder().setX(pt.getX()).setY(pt.getY()).build();
	}
	
	public static Envelope fromProto(EnvelopeProto proto) {
		return new Envelope(fromProto(proto.getTl()), fromProto(proto.getBr()));
	}
	
	public static EnvelopeProto toProto(Envelope envl) {
		return EnvelopeProto.newBuilder()
							.setTl(CoordinateProto.newBuilder()
												.setX(envl.getMinX())
												.setY(envl.getMinY())
												.build())
							.setBr(CoordinateProto.newBuilder()
												.setX(envl.getMaxX())
												.setY(envl.getMaxY())
												.build())
							.build();
	}
	
	public static Geometry fromProto(GeometryProto proto) {
		switch ( proto.getEitherCase() ) {
			case POINT:
				return fromProto(proto.getPoint());
			case WKB:
				try {
					return GeoClientUtils.fromWKB(proto.getWkb().toByteArray());
				}
				catch ( ParseException e ) {
					throw new IllegalArgumentException("invalid WKB: cause=" + e);
				}
			case NULL:
				return null;
			case EMPTY:
				TypeCode tc = TypeCode.fromCode(proto.getEmpty().getNumber());
				GeometryDataType dt = (GeometryDataType)DataTypes.fromTypeCode(tc);
				return GeoClientUtils.emptyGeometry(dt.toGeometries());
			default:
				throw new AssertionError();
		}
	}
	
	public static TypeCodeProto toProto(TypeCode tc) {
		return TypeCodeProto.forNumber(tc.get());
	}
	public static TypeCode fromProto(TypeCodeProto proto) {
		return TypeCode.fromCode(proto.getNumber());
	}

	private static final GeometryProto NULL_GEOM = GeometryProto.newBuilder().setNull(VOID).build();
	public static GeometryProto toProto(Geometry geom) {
		if ( geom == null ) {
			return NULL_GEOM;
		}
		else if ( geom.isEmpty() ) {
			TypeCode tc = GeometryDataType.fromGeometry(geom).getTypeCode();
			TypeCodeProto tcProto = TypeCodeProto.valueOf(tc.name());
			return GeometryProto.newBuilder().setEmpty(tcProto).build();
		}
		else if ( geom instanceof Point ) {
			Point pt = (Point)geom;
			PointProto ptProto = PointProto.newBuilder()
											.setX(pt.getX())
											.setY(pt.getY())
											.build();
			return GeometryProto.newBuilder().setPoint(ptProto).build();
		}
		else {
			ByteString wkb = ByteString.copyFrom(GeoClientUtils.toWKB(geom));
			return GeometryProto.newBuilder().setWkb(wkb).build();
		}
	}
	
	public static Throwable parsePlanExecutionError(ErrorProto proto) {
		switch ( proto.getCode() ) {
			case ERROR_PLAN_EXECUTION_INTERRUPTED:
				return new InterruptedException(proto.getDetails());
			case ERROR_PLAN_EXECUTION_CANCELLED:
				return new CancellationException(proto.getDetails());
			case ERROR_PLAN_EXECUTION_FAILED:
				return new ExecutionException(new PlanExecutionException(proto.getDetails()));
			case ERROR_PLAN_EXECUTION_TIMED_OUT:
				return new TimeoutException(proto.getDetails());
			default:
				return PBUtils.toException(proto);
		}
	}
	
	public static Result<Void> fromProto(ResultProto proto) {
		switch ( proto.getEitherCase() ) {
			case VALUE:
				return Result.some(null);
			case FAILURE:
				return Result.failure(parsePlanExecutionError(proto.getFailure()));
			case NONE:
				return  Result.none();
			default:
				throw new AssertionError();
		}
	}
	
	public static ResultProto toProto(Result<Void> result) {
		ResultProto.Builder builder = ResultProto.newBuilder();
		if ( result.isSuccess() ) {
			builder.setValue(PBUtils.toValueProto(null));
		}
		else if ( result.isFailure() ) {
			builder.setFailure(PBUtils.toErrorProto(result.getCause()));
		}
		else {
			builder.setNone(PBUtils.VOID);
		}
		return builder.build();
	}
	
	public static Map<String,String> fromProto(PropertiesProto proto) {
		return FStream.from(proto.getPropertyList())
				.fold(Maps.newHashMap(), (map,kv) -> {
					map.put(kv.getKey(), kv.getValue());
					return map;
				});
	}
	
	public static PropertiesProto toProto(Map<String,String> metadata) {
		List<PropertyProto> properties = KVFStream.from(metadata)
												.map(kv -> PropertyProto.newBuilder()
																		.setKey(kv.key())
																		.setValue(kv.value())
																		.build())
												.toList();
		return PropertiesProto.newBuilder()
							.addAllProperty(properties)
							.build();
	}
	
	public static RecordProto toProto(Record record) {
		return FStream.of(record.getAll())
						.map(PBUtils::toValueProto)
						.fold(RecordProto.newBuilder(), (b,p) -> b.addColumn(p))
						.build();
	}
	
	public static void fromProto(Record output, RecordProto proto) {
		Object[] values = proto.getColumnList().stream()
								.map(PBUtils::fromProto)
								.toArray();
		output.setAll(values);
	}
	
	public static Record fromProto(RecordSchema schema, RecordProto proto) {
		Object[] values = proto.getColumnList().stream()
								.map(PBUtils::fromProto)
								.toArray();
		Record output = DefaultRecord.of(schema);
		output.setAll(values);
		
		return output;
	}
	
	public static Map<String,Object> fromProto(KeyValueMapProto kvmProto) {
		return FStream.from(kvmProto.getKeyValueList())
					.toKeyValueStream(KeyValueProto::getKey, KeyValueProto::getValue)
					.mapValue(vproto -> fromProto(vproto)._2)
					.toMap();
	}
	
	public static KeyValueMapProto toKeyValueMapProto(Map<String,Object> keyValueMap) {
		List<KeyValueProto> keyValues = KVFStream.from(keyValueMap)
												.map(kv -> KeyValueProto.newBuilder()
																	.setKey(kv.key())
																	.setValue(toValueProto(kv.value()))
																	.build())
												.toList();
		return KeyValueMapProto.newBuilder()
							.addAllKeyValue(keyValues)
							.build();
	}
	
//	private static final int TOO_BIG = (int)UnitUtils.parseByteSize("25mb");
	private static final int STRING_COMPRESS_THRESHOLD = (int)UnitUtils.parseByteSize("1mb");
	private static final int BINARY_COMPRESS_THRESHOLD = (int)UnitUtils.parseByteSize("4mb");
	public static ValueProto toValueProto(TypeCode tc, Object obj) {
		if ( obj == null ) {
			return ValueProto.newBuilder()
							.setNullValue(TypeCodeProto.valueOf(tc.name()))
							.build();
		}
		
		ValueProto.Builder builder = ValueProto.newBuilder();
		switch ( tc ) {
			case BYTE:
				builder.setByteValue((byte)obj);
				break;
			case SHORT:
				builder.setShortValue((short)obj);
				break;
			case INT:
				builder.setIntValue((int)obj);
				break;
			case LONG:
				builder.setLongValue((long)obj);
				break;
			case FLOAT:
				builder.setFloatValue((float)obj);
				break;
			case DOUBLE:
				builder.setDoubleValue((double)obj);
				break;
			case BOOLEAN:
				builder.setBoolValue((boolean)obj);
				break;
			case STRING:
				String str = ((String)obj);
				if ( str.length() < STRING_COMPRESS_THRESHOLD ) {
					builder.setStringValue(str);
				}
				else {
					try {
						byte[] compressed = IOUtils.compress(str.getBytes());
//						if ( compressed.length > TOO_BIG ) {
//							throw new PBException("string value is too big: size="
//												+ UnitUtils.toByteSizeString(compressed.length));
//						}
						builder.setCompressedStringValue(ByteString.copyFrom(compressed));
					}
					catch ( IOException e ) {
						throw new PBException(e);
					}
				}
				break;
			case BINARY:
				byte[] bytes = (byte[])obj;
				if ( bytes.length < BINARY_COMPRESS_THRESHOLD ) {
					builder.setBinaryValue(ByteString.copyFrom(bytes));
				}
				else {
					try {
						byte[] compressed = IOUtils.compress(bytes);
//						if ( compressed.length > TOO_BIG ) {
//							throw new PBException("binary value is too big: size="
//												+ UnitUtils.toByteSizeString(compressed.length));
//						}
						builder.setCompressedBinaryValue(ByteString.copyFrom(compressed));
					}
					catch ( IOException e ) {
						throw new PBException(e);
					}
				}
				break;
			case DATETIME:
				builder.setDatetimeValue(DateTimeFunctions.DateTimeToMillis(obj));
				break;
			case DATE:
				builder.setDateValue(DateFunctions.DateToMillis(obj));
				break;
			case TIME:
				builder.setTimeValue(TimeFunctions.TimeToString(obj));
				break;
			case INTERVAL:
				builder.setIntervalValue(toProto((Interval)obj));
				break;
			case ENVELOPE:
				builder.setEnvelopeValue(PBUtils.toProto((Envelope)obj));
				break;
			case TILE:
				MapTile tile = (MapTile)obj;
				builder.setTileValue(MapTileProto.newBuilder()
												.setX(tile.getX())
												.setY(tile.getY())
												.setZoom(tile.getZoom())
												.build());
				break;
			case GRID_CELL:
				GridCell cell = (GridCell)obj;
				builder.setGridCellValue(GridCellProto.newBuilder()
														.setX(cell.getX())
														.setY(cell.getY())
														.build());
				break;
			case POINT:
				builder.setPointValue(PBUtils.toProto((Point)obj));
				break;
			case MULTI_POINT:
			case LINESTRING:
			case MULTI_LINESTRING:
			case POLYGON:
			case MULTI_POLYGON:
			case GEOM_COLLECTION:
			case GEOMETRY:
				builder.setGeometryValue(PBUtils.toProto((Geometry)obj));
				break;
			case TRAJECTORY:
				builder.setTrajectoryValue(((Trajectory)obj).toProto());
				break;
			default:
				throw new AssertionError();
		}
		
		return builder.build();
	}
	
	public static Tuple2<DataType,Object> fromProto(ValueProto proto) {
		switch ( proto.getValueCase() ) {
			case BYTE_VALUE:
				return Tuple.of(DataType.BYTE, (byte)proto.getByteValue());
			case SHORT_VALUE:
				return Tuple.of(DataType.SHORT, (short)proto.getShortValue());
			case INT_VALUE:
				return Tuple.of(DataType.INT, (int)proto.getIntValue());
			case LONG_VALUE:
				return Tuple.of(DataType.LONG, proto.getLongValue());
			case FLOAT_VALUE:
				return Tuple.of(DataType.FLOAT, proto.getFloatValue());
			case DOUBLE_VALUE:
				return Tuple.of(DataType.DOUBLE, proto.getDoubleValue());
			case BOOL_VALUE:
				return Tuple.of(DataType.BOOLEAN, proto.getBoolValue());
			case STRING_VALUE:
				return Tuple.of(DataType.STRING, proto.getStringValue());
			case COMPRESSED_STRING_VALUE:
				try {
					byte[] bytes = proto.getCompressedStringValue().toByteArray();
					bytes = IOUtils.decompress(bytes);
					return Tuple.of(DataType.STRING, new String(bytes));
				}
				catch ( Exception e ) {
					throw new PBException(e);
				}
			case BINARY_VALUE:
				return Tuple.of(DataType.BINARY, proto.getBinaryValue().toByteArray());
			case COMPRESSED_BINARY_VALUE:
				try {
					byte[] bytes = proto.getCompressedBinaryValue().toByteArray();
					return Tuple.of(DataType.BINARY, IOUtils.decompress(bytes));
				}
				catch ( Exception e ) {
					throw new PBException(e);
				}
			case DATETIME_VALUE:
				return Tuple.of(DataType.DATETIME, DateTimeFunctions.DateTimeFromMillis(proto.getDatetimeValue()));
			case DATE_VALUE:
				return Tuple.of(DataType.DATE, DateFunctions.DateFromMillis(proto.getDateValue()));
			case TIME_VALUE:
				return Tuple.of(DataType.TIME, TimeFunctions.TimeFromString(proto.getTimeValue()));
			case DURATION_VALUE:
				throw new UnsupportedOperationException("duration type");
			case INTERVAL_VALUE:
				IntervalProto intvlProto = proto.getIntervalValue();
				return Tuple.of(DataType.INTERVAL,
							Interval.between(intvlProto.getStart(), intvlProto.getEnd()));
			case ENVELOPE_VALUE:
				return Tuple.of(DataType.ENVELOPE, PBUtils.fromProto(proto.getEnvelopeValue()));
			case TILE_VALUE:
				MapTileProto mtp = proto.getTileValue();
				return Tuple.of(DataType.TILE, new MapTile(mtp.getZoom(), mtp.getX(), mtp.getY()));
			case GRID_CELL_VALUE:
				GridCellProto gcProto = proto.getGridCellValue();
				return Tuple.of(DataType.GRID_CELL, new GridCell(gcProto.getX(), gcProto.getY()));
			case POINT_VALUE:
				return Tuple.of(DataType.POINT, PBUtils.fromProto(proto.getPointValue()));
			case GEOMETRY_VALUE:
				Geometry geom = PBUtils.fromProto(proto.getGeometryValue());
				DataType type = GeometryDataType.fromGeometry(geom);
				return Tuple.of(type, geom);
			case TRAJECTORY_VALUE:
				Trajectory trj = Trajectory.fromProto(proto.getTrajectoryValue());
				return Tuple.of(DataType.TRAJECTORY, trj);
			case NULL_VALUE:
				TypeCode tc = TypeCode.valueOf(proto.getNullValue().name());
				return Tuple.of(DataTypes.fromTypeCode(tc), null);
			case VALUE_NOT_SET:
				return Tuple.of(null, null);
			default:
				throw new AssertionError();
		}
	}
	
	public static ValueProto toValueProto(Object obj) {
		if ( obj == null ) {
			return ValueProto.newBuilder().build();
		}
		
		ValueProto.Builder builder = ValueProto.newBuilder();
		if ( obj instanceof String ) {
			builder.setStringValue((String)obj);
		}
		else if ( obj instanceof Integer ) {
			builder.setIntValue((int)obj);
		}
		else if ( obj instanceof Double ) {
			builder.setDoubleValue((double)obj);
		}
		else if ( obj instanceof Long ) {
			builder.setLongValue((long)obj);
		}
		else if ( obj instanceof Boolean ) {
			builder.setBoolValue((boolean)obj);
		}
		else if ( obj instanceof Point ) {
			builder.setPointValue(PBUtils.toProto((Point)obj));
		}
		else if ( obj instanceof Geometry ) {
			builder.setGeometryValue(PBUtils.toProto((Geometry)obj));
		}
		else if ( obj instanceof Envelope ) {
			builder.setEnvelopeValue(PBUtils.toProto((Envelope)obj));
		}
		else if ( obj instanceof Byte[] ) {
			builder.setBinaryValue(ByteString.copyFrom((byte[])obj));
		}
		else if ( obj instanceof Byte ) {
			builder.setByteValue((byte)obj);
		}
		else if ( obj instanceof Short ) {
			builder.setShortValue((short)obj);
		}
		else if ( obj instanceof Float ) {
			builder.setFloatValue((float)obj);
		}
		else if ( obj instanceof LocalDateTime ) {
			builder.setDatetimeValue(DateTimeFunctions.DateTimeToMillis(obj));
		}
		else if ( obj instanceof LocalDate ) {
			builder.setDateValue(DateFunctions.DateToMillis(obj));
		}
		else if ( obj instanceof LocalTime ) {
			builder.setTimeValue(TimeFunctions.TimeToString(obj));
		}
		else if ( obj instanceof MapTile ) {
			MapTile tile = (MapTile)obj;
			builder.setTileValue(MapTileProto.newBuilder()
											.setX(tile.getX())
											.setY(tile.getY())
											.setZoom(tile.getZoom())
											.build());
		}
		else if ( obj instanceof GridCell ) {
			GridCell cell = (GridCell)obj;
			builder.setGridCellValue(GridCellProto.newBuilder()
													.setX(cell.getX())
													.setY(cell.getY())
													.build());
		}
		else if ( obj instanceof Trajectory ) {
			builder.setTrajectoryValue(((Trajectory)obj).toProto());
		}
		else {
			throw new AssertionError();
		}
		
		return builder.build();
	}
*/
}
