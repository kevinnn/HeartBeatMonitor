package sa.sse.ustc;

public class HeartBeatMsg {
	
	
	private boolean isForce;			// 是否是服务器强制查询客户端,默认是false
	private boolean isResponse;			// 是否是响应报文
	private boolean isNormal;			// 是否正常工作
	private short exCode;				// 发生异常时候的异常码
	private int dataNodeNumber;			// DataNode ID
	private short dataNodeGroupNumber;	// DataNode Group ID
	private short interval;				// HeartBeat 时间间隔
	
	public HeartBeatMsg(boolean isForce, boolean isResponse, boolean isNormal, short exCode, int dataNodeNumber,
			short dataNodeGroupNumber, short interval) throws IllegalArgumentException {
		if (exCode < 0) {
			throw new IllegalArgumentException("Bad Exception Code, Must be positive: " + exCode);
		}
		if (exCode > 127) {
			throw new IllegalArgumentException("Bad Exception Code, More than 127: " + exCode);
		}
		if (dataNodeNumber < 0) {
			throw new IllegalArgumentException("Bad DataNode Number, Must be positive: " + dataNodeNumber);
		}
		if (dataNodeGroupNumber < 0) {
			throw new IllegalArgumentException("Bad DataNode GroupNumber, Must be positive: " + dataNodeGroupNumber);
		}
		if (interval < 0) {
			throw new IllegalArgumentException("Bad Interval time, Must be positive: " + interval);
		}
		this.isForce = isForce;
		this.isResponse = isResponse;
		this.isNormal = isNormal;
		this.exCode = exCode;
		this.dataNodeNumber = dataNodeNumber;
		this.dataNodeGroupNumber = dataNodeGroupNumber;
		this.interval = interval;
	}

	public boolean isForce() {
		return isForce;
	}

	public void setForce(boolean isForce) {
		this.isForce = isForce;
	}

	public boolean isResponse() {
		return isResponse;
	}

	public void setResponse(boolean isResponse) {
		this.isResponse = isResponse;
	}

	public boolean isNormal() {
		return isNormal;
	}

	public void setNormal(boolean isNormal) {
		this.isNormal = isNormal;
	}

	public short getExCode() {
		return exCode;
	}

	public void setExCode(short exCode) throws IllegalArgumentException {
		if (exCode < 0) {
			throw new IllegalArgumentException("Bad Exception Code, Must be positive: " + exCode);
		}
		this.exCode = exCode;
	}

	public int getDataNodeNumber() {
		return dataNodeNumber;
	}

	public void setDataNodeNumber(int dataNodeNumber) throws IllegalArgumentException {
		if (dataNodeNumber < 0) {
			throw new IllegalArgumentException("Bad DataNode Number, Must be positive: " + dataNodeNumber);
		}
		this.dataNodeNumber = dataNodeNumber;
	}

	public short getDataNodeGroupNumber() {
		return dataNodeGroupNumber;
	}

	public void setDataNodeGroupNumber(short dataNodeGroupNumber) throws IllegalArgumentException {
		if (dataNodeGroupNumber < 0) {
			throw new IllegalArgumentException("Bad DataNode GroupNumber, Must be positive: " + dataNodeGroupNumber);
		}
		this.dataNodeGroupNumber = dataNodeGroupNumber;
	}

	public short getInterval() {
		return interval;
	}

	public void setInterval(short interval) throws IllegalArgumentException {
		if (interval < 0) {
			throw new IllegalArgumentException("Bad Interval time, Must be positive: " + interval);
		}
		this.interval = interval;
	}

	@Override
	public String toString() {
		return "HeartBeatMsg [isForce=" + isForce + ", isResponse=" + isResponse + ", isNormal=" + isNormal
				+ ", exCode=" + exCode + ", dataNodeNumber=" + dataNodeNumber + ", dataNodeGroupNumber="
				+ dataNodeGroupNumber + ", interval=" + interval + "]";
	}
	
	
}
