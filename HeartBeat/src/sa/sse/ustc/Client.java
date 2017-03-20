package sa.sse.ustc;

import java.net.Socket;
import java.sql.Timestamp;

public class Client extends DBUtil {
	private Socket clientConnectSocket = null;
	
	private long timer = -1;
	private boolean isDead = false;		// 是否长时间未发送心跳包，超时则表明死亡，移除该DataNode
	private int clientHeartPort = -1;
	// 存储 DataNode 的各种状态
	private short dataNodeGroupNumber;	// DataNode Group ID
	private int dataNodeNumber;			// DataNode ID
	private boolean isNormal;			// 是否正常工作
	private short exCode;				// 发生异常时候的异常码
	private short interval;				// HeartBeat 时间间隔
	
	public boolean removeEntity() {
		try {
			execSQL("delete from datanodeStatus where groupNumber = ? and nodeNumber = ?", dataNodeGroupNumber, dataNodeNumber);
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		} finally {
			closeConnection();
		}
		return true;
	}
	
	public boolean insertEntity() {
		try {
			execSQL("replace into datanodeStatus values (?,?,?,?,?,?,?,?,?)", 
					dataNodeGroupNumber, dataNodeNumber, isNormal, exCode, interval, 
					getClientHost(), getClientConnectSocketPort(), getClientHeartPort(), new Timestamp(System.currentTimeMillis()));
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		} finally {
			closeConnection();
		}
		return true;
	}
	
	public boolean updateEntity() {
		return insertEntity();
	}
	
	
	public Client(Socket clientConnectSocket, int clientHeartPort, short dataNodeGroupNumber, int dataNodeNumber,
			boolean isNormal, short exCode, short interval, long timer) {
		this.timer = timer;
		this.clientConnectSocket = clientConnectSocket;
		this.clientHeartPort = clientHeartPort;
		this.dataNodeGroupNumber = dataNodeGroupNumber;
		this.dataNodeNumber = dataNodeNumber;
		this.isNormal = isNormal;
		this.exCode = exCode;
		this.interval = interval;
	}

	public void setDead(boolean dead) {
		synchronized(this) {
			this.isDead = dead;
		}
	}
	
	public boolean isDead() {
		return this.isDead;
	}
	
	public void setTimer(long timer) {
		synchronized(this) {
			this.timer = timer;
		}
		
	}
	
	public long getTimer() {
		return this.timer;
	}
	
	public Socket getClientSocket() {
		return clientConnectSocket;
	}

	public void setClientSocket(Socket clientSocket) {
		synchronized(this) {
			this.clientConnectSocket = clientSocket;
		}
		
	}
	
	public String getClientHost() {
		if (clientConnectSocket != null) {
			return clientConnectSocket.getInetAddress().getHostAddress(); 
		}
		return null;
	}
	
	public int getClientConnectSocketPort() {
		if (clientConnectSocket != null) {
			return clientConnectSocket.getPort(); 
		}
		return -1;
	}
	
	public int getClientHeartPort() {
		if (clientHeartPort != -1) {
			return clientHeartPort; 
		}
		return -1;
	}
	
	public void setClientHeartPort(int clientHeartPort) {
		synchronized(this) {
			this.clientHeartPort = clientHeartPort;
		}
		
	}


	public short getDataNodeGroupNumber() {
		return dataNodeGroupNumber;
	}


	public void setDataNodeGroupNumber(short dataNodeGroupNumber) {
		synchronized(this) {
			this.dataNodeGroupNumber = dataNodeGroupNumber;
		}
		
	}


	public int getDataNodeNumber() {
		return dataNodeNumber;
	}


	public void setDataNodeNumber(int dataNodeNumber) {
		synchronized(this) {
			this.dataNodeNumber = dataNodeNumber;
		}
		
	}


	public boolean isNormal() {
		return isNormal;
	}


	public void setNormal(boolean isNormal) {
		synchronized(this) {
			this.isNormal = isNormal;
		}
		
	}


	public short getExCode() {
		return exCode;
	}


	public void setExCode(short exCode) {
		synchronized(this) {
			this.exCode = exCode;
		}
		
	}


	public short getInterval() {
		return interval;
	}


	public void setInterval(short interval) {
		synchronized(this) {
			this.interval = interval;
		}
		
	}
	
}