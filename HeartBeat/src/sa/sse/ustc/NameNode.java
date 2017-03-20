package sa.sse.ustc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.logging.Logger;

public class NameNode {
	private volatile static NameNode nameNode = null;
	private static int CONNECT_PORT = 9999; 		// 连接端口
	private static int HEARTBEAT_PORT = 9998; 		// 心跳端口，这个版本使用基于udp的心跳协议
	private static short groupCount = 0; 			// 用于给DataNode分配组号
	private static int nodeCount = 0; 				// 用于给DataNode分配编号
	private static int MAX_NODES = 3;				// 最大编码限制
	private static int MAX_GROUPS = 8;				// 最大组号限制
	private List<Client> dataNodes = new ArrayList<Client>(); // 维护DataNode的一个集合，数据库缓存
	private Object dataNodesLock = new Object();	// dataNodes集合的简单读写锁
	private ServerSocket ss = null;
	private DatagramSocket ds = null;
	
	private final static Logger LOGGER = Logger.getLogger(NameNode.class.getName());
	
	public static void main(String[] args) {
		NameNode.getInstance().run();
	}
	
	private NameNode() {}
	
	// 单例模式访问点
	public static NameNode getInstance() {
		if (nameNode == null) {
			synchronized(NameNode.class) {
				if (nameNode == null) {
					nameNode = new NameNode();
				}
			}
		}
		return nameNode;
	}
	
	public List<Client> getClients() {
		if (dataNodes.isEmpty())
			return null;
		return dataNodes;
	}
	private class Task implements Runnable {
		Client client = null;

		public Task(Client client) {
			this.client = client;
		}

		@Override
		public void run() {
			PrintStream out = null;

			BufferedReader buf = null;
			try {
				out = new PrintStream(client.getClientSocket().getOutputStream());

				buf = new BufferedReader(new InputStreamReader(client.getClientSocket().getInputStream()));
				//首先读取DataNode连接之后发来的HeartBeat UDP端口号
				int clientHeartPort = Integer.parseInt(buf.readLine());
				//立即设置服务器维护的DataNode的udp端口，用来在HeartBeatDUP线程中给对应的DataNode发送HeartBeat response包
				client.setClientHeartPort(clientHeartPort);
				//System.out.println("DataNode "+ client.getClientHost() + ":" + client.getClientConnectSocketPort() + " Sent: " + clientHeartPort);
				LOGGER.info("DataNode "+ client.getClientHost() + ":" + client.getClientConnectSocketPort() + " Sent: " + clientHeartPort);
				//回复给DataNode自己收到UDP端口
				out.println("ok, udp port accepted");
				//然后服务器告知给DataNode分配的GroupNumber，DataNodeNumber，需要发送心跳的时间间隔Interval
				out.println("" + client.getDataNodeGroupNumber());
				out.println("" + client.getDataNodeNumber());
				out.println("" + client.getInterval());
				// 最后读取确认信息，完成协商
				String resp = buf.readLine();
				if (!"ok, ID accepted".equals(resp)) {
					throw new IOException("Wrong DataNode ACK");
				}
				//System.out.println("DataNode "+ client.getClientHost() + ":" + client.getClientConnectSocketPort() + " ACK: " + resp);
				LOGGER.info("DataNode "+ client.getClientHost() + ":" + client.getClientConnectSocketPort() + " ACK: " + resp);
				boolean flag = true;
				while (flag) {
					String msg = buf.readLine();
					if (msg == null || "bye".equals(msg)) {
						flag = false;
					} else if ("".equals(msg)) {
						out.println("Please input some message!");
					} else {
						out.println(msg);
					}
				}

			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				out.close();
				if (client.getClientSocket() != null) {
					try {
						client.getClientSocket().close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}

		}

	}
	
	//2. 需要对每个已连接的DataNode设置定时器，如果规定时间内未收到HeartBeat，则认为该DataNode死亡
	// 这里出现读写模型了，TimeOutChecker读dataNodes， 而HeartBeatUDP写dataNodes;因此考虑吧Client写成同步模型，并对dataNodes集合的访问枷锁，但是可能会大幅降低性能，有可能造成延迟到时定时器更加不准确
	private class TimeOutChecker extends Thread {
		long timeout = 120;			// 超时时间,单位s
		@Override
		public void run() {
			//System.out.println("TimeOutChecker thread started......");
			LOGGER.info("TimeOutChecker thread started......");
			while (true) {
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				long now = System.currentTimeMillis()/1000;
				synchronized(dataNodesLock) {
					for (Iterator<Client> it = dataNodes.listIterator(); it.hasNext(); ) {
						Client dataNode = it.next();
						// 对于每一个已经连接的dataNode，通过每隔 5s 插入数据库，如果是已经存在，直接覆盖，不不存在则插入
						dataNode.insertEntity();
						if (now - dataNode.getTimer() > timeout) {
							dataNode.setDead(true);
//							System.out.println("A DataNode " + dataNode.getDataNodeGroupNumber()
//							+ "-" + dataNode.getDataNodeNumber()+ " " + dataNode.getClientHost() + ":" + dataNode.getClientHeartPort()
//							+ " is Dead, it will be cleared");
							LOGGER.info("A DataNode " + dataNode.getDataNodeGroupNumber()
							+ "-" + dataNode.getDataNodeNumber()+ " " + dataNode.getClientHost() + ":" + dataNode.getClientHeartPort()
							+ " is Dead, it will be cleared");
							// 同步数据库
							dataNode.removeEntity();
							// 从缓存移除
							it.remove();
						}
					}
				}	
			}
			
		}
		
	}
	
	//1. NameNode端只使用一个线程处理所有DataNode的HeartBeat心跳包，收到DataNode的心跳包之后，解析该DataNode状态，并返回给对应DataNode一个response心跳包
	//2. 需要对每个已连接的DataNode设置定时器，如果规定时间内未收到HeartBeat，则认为该DataNode死亡
	private class HeartBeatUDP extends Thread {

		byte[] buf = new byte[10];
		
		@Override
		public void run() {
			
			//System.out.println("HeartBeatUDP thread started at server udpPort: " + HEARTBEAT_PORT);
			LOGGER.info("HeartBeatUDP thread started at server udpPort: " + HEARTBEAT_PORT);
			DatagramPacket dp = new DatagramPacket(buf, buf.length);
			while (ds != null) {
				try {
					ds.receive(dp);
					HeartBeatMsg from = parse(dp);
					String dataNodeHost = dp.getAddress().getHostAddress();
					int dataNodePort = dp.getPort();
					// 更新DataNode状态信息，并对收到HeartBeat的DataNode重置定时器
					synchronized (dataNodesLock) {
						for (Iterator<Client> it = dataNodes.iterator(); it.hasNext(); ) {
							Client dataNode = it.next();
							if (from.getDataNodeGroupNumber() == dataNode.getDataNodeGroupNumber() && 
									from.getDataNodeNumber() == dataNode.getDataNodeNumber()) {
									dataNode.setExCode(from.getExCode());
									dataNode.setInterval(from.getInterval());
									dataNode.setClientHeartPort(dp.getPort());
									long now = System.currentTimeMillis()/1000;
									dataNode.setTimer(now);
								
//									System.out.println("A HeartBeat DatagramPacket received from DataNode " + dataNode.getDataNodeGroupNumber()
//									+ "-" + dataNode.getDataNodeNumber()+ " " + dataNodeHost + ":" + dataNodePort
//									+ " " + from.toString());
									
									LOGGER.info("A HeartBeat DatagramPacket received from DataNode " + dataNode.getDataNodeGroupNumber()
									+ "-" + dataNode.getDataNodeNumber()+ " " + dataNodeHost + ":" + dataNodePort
									+ " " + from.toString());
							}
							
						}
					}
						
					
					HeartBeatMsg to = new HeartBeatMsg(false,true,true,from.getExCode(),
							from.getDataNodeNumber(),
							from.getDataNodeGroupNumber(),
							from.getInterval());
					ds.send(wrap(to, dataNodeHost, dataNodePort));
					
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}

		}

	}

	public void run() {
		//System.out.println("NameNode started at server tcpPort: " + CONNECT_PORT + "......");
		LOGGER.info("NameNode started at server tcpPort: " + CONNECT_PORT + "......");
		Socket clientSocket = null;
		// 打开UDP端口，用来收发心跳包
		try {
			ds = new DatagramSocket(HEARTBEAT_PORT);
		} catch (SocketException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// 启动HeartBeatUDP线程
		new HeartBeatUDP().start();
		// 启动TimeOutChecker线程
		new TimeOutChecker().start();
		try {

			ss = new ServerSocket(CONNECT_PORT);

			while (true) {
				clientSocket = ss.accept();
//				System.out.println(
//						"DataNode " + clientSocket.getInetAddress().getHostAddress() + ":" + clientSocket.getPort() + " connected");
				
				LOGGER.info(
						"DataNode " + clientSocket.getInetAddress().getHostAddress() + ":" + clientSocket.getPort() + " connected");
				//还无法及时知道客户端UDP端口heartBeatPort，这里先定为-1， 需要在Task线程中收到消息之后才能确定
				Client dataNode = new Client(clientSocket, -1, groupCount, nodeCount, true, (short)0, (short)5, System.currentTimeMillis()/1000);
				synchronized(dataNodesLock){
					dataNodes.listIterator().add(dataNode);
					//dataNodes.add(dataNode); // 多线程环境最好不要使用集合的add方法
				}	
				new Thread(new Task(dataNode)).start();
				nodeCount++;
				if (nodeCount == MAX_NODES) {
					nodeCount = 0;
					groupCount++;
					if (groupCount == MAX_GROUPS) {
						throw new Exception("groups too many！");
					}
				}
			}
		} catch (IOException e) {
			System.out.println(e.toString());
		} catch (Exception e) {
			System.out.println(e.toString());
		} finally {
			try {
				if (ss != null) {
					ss.close();
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	private HeartBeatMsg parse(DatagramPacket dp) throws IOException{
//		System.out.println(dp.getLength());
//		System.out.println(dp.getData().length);
		return new HeartBeatMsgBinCoder().fromWire(dp.getData());
	}
	
	private DatagramPacket wrap(HeartBeatMsg msg, String host, int port) {
		byte[] data = null;
		try {
			data = new HeartBeatMsgBinCoder().toWire(msg);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return new DatagramPacket(data, data.length, new InetSocketAddress(host, port));
	}
}
