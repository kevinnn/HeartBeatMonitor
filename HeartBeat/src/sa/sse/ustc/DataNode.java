package sa.sse.ustc;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;

public class DataNode {
	private static String SERVER_HOST = "127.0.0.1";
	private static int CONNECT_SERVER_PORT = 9999;		// SERVER TCP PORT 
	private static int HEARTBEAT_SERVER_PORT = 9998; 	// SERVER UDP PORT
	private static int HEARTBEAT_CLIENT_PORT = 7777;	// CLIENT UDP PORT AUTOINCREMENT
	private DatagramSocket ds = null;
	
	// 存储 DataNode 的各种状态
	private short dataNodeGroupNumber;					// DataNode Group ID
	private int dataNodeNumber;							// DataNode ID
	private volatile boolean isNormal = true;					// 是否正常工作，默认正常工作
	private volatile short exCode = 0;							// 发生异常时候的异常码,默认无异常
	private volatile short interval;								// HeartBeat 时间间隔

	public DataNode() {
		// DataNode在构造时，必须先打开UDP端口，这样在与服务器建立链接之后，才有可能及时收到服务器的UDP包
		// 为了防止多个DataNode占用同一个UDP端口，这里使用static变量自增。
		try {
			ds = new DatagramSocket(HEARTBEAT_CLIENT_PORT++);
		} catch (SocketException e) {
			e.printStackTrace();
		}
	}
	
	private void connect() {
		Socket server = null;
		BufferedReader userinput = null;
		PrintStream out = null;
		BufferedReader buf = null;
		try {
			server = new Socket(SERVER_HOST, CONNECT_SERVER_PORT);
			userinput = new BufferedReader(new InputStreamReader(System.in));
			
			out = new PrintStream(server.getOutputStream());
			
			//BufferedInputStream bis = new BufferedInputStream(server.getInputStream());
			buf = new BufferedReader(new InputStreamReader(server.getInputStream())); 
			// DataNode建立连接后首先报告自己的HeartBeat UDP端口给NameNode
			out.println("" + (HEARTBEAT_CLIENT_PORT-1));
			// 读取来自NameNode的回复
			String resp = buf.readLine();
			if (!"ok, udp port accepted".equals(resp)) {
				throw new IOException("Wrong NameNode ACK");
			}
			System.out.println("NameNode ACK: " + resp);
			// 接着读取NameNode分配的GroupNumber，DataNodeNumber，需要发送心跳的时间间隔Interval
			dataNodeGroupNumber = Short.parseShort(buf.readLine());
			dataNodeNumber = Integer.parseInt(buf.readLine());
			interval = Short.parseShort(buf.readLine());
			System.out.println("NameNode Sent: " + "dataNodeGroupNumber: " + dataNodeGroupNumber + "\t" +
								"dataNodeNumber: " + dataNodeNumber + "\t" +
								"interval: " + interval);
			// 最后完成协议，告知NameNode自己已经拿到GroupNumber，DataNodeNumber，Interval了
			out.println("ok, ID accepted");
			// 启动心跳线程，主动定时向NameNode发送心跳包,这里采用的是DataNode主动发送而不是NameNode询问
			HeartBeatUDP heartBeat = new HeartBeatUDP();
			heartBeat.start();
			try {
				Thread.sleep(5);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			boolean flag = true;
			while (flag) {
				System.out.println("Input Message： ");
				String msg = userinput.readLine();
				// 模拟异常
				if ("broken".equals(msg)) {
					isNormal = false;
					exCode = 1;
				}
				if ("killed".equals(msg)) {
					isNormal = false;
					exCode = 2;
				}
				if ("fine".equals(msg)) {
					isNormal = true;
					exCode = 0;
				}
				// 发送给服务端
				out.println(msg);
				// 正常关机
				if ("bye".equals(msg)) {
					isNormal = true;
					exCode = 0;
					flag = false;
				}
				else {
					String echo = buf.readLine();
					System.out.println("Server: " + echo);
				}
			}
			// 正常断开连接， 结束心跳线程
			heartBeat.cancel();
		} catch (SocketTimeoutException e) {
			System.out.println("Time out, No response");
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				userinput.close();
				if (server != null) {
					server.close();
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	private class HeartBeatUDP extends Thread {

		byte[] buf = new byte[10];
		boolean flag = true;
		@Override
		public void run() {
			
			System.out.println("HeartBeatUDP thread started at DataNode udpPort: " + (HEARTBEAT_CLIENT_PORT-1));
			DatagramPacket dp = new DatagramPacket(buf, buf.length);
			while (flag) {
				try {
					// 客户端延迟Interval秒再发心跳包
					Thread.sleep(interval*1000);
					HeartBeatMsg heartBeatMsgToServer = new HeartBeatMsg(false,false,isNormal,exCode,
																			dataNodeNumber,
																			dataNodeGroupNumber,
																			interval);
					ds.send(wrap(heartBeatMsgToServer, SERVER_HOST, HEARTBEAT_SERVER_PORT));
					
					ds.receive(dp);
					HeartBeatMsg heartBeatMsgFromServer = parse(dp);
					//System.out.println("A DatagramPacket received from NameNode " + heartBeatMsgFromServer.toString());
						
				} catch (IOException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
					return;
				}

			}

		}
		
		// kill this thread
		public void cancel() { this.flag = false; }

	}
	
	private HeartBeatMsg parse(DatagramPacket dp) throws IOException{
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
	
	public static void main (String[] args) {
			new DataNode().connect();// while里面阻塞的，后面的无法执行
	}
}
