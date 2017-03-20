package sa.sse.ustc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class HeartBeatMsgBinCoder implements HeartBeatMsgCoder {

	public static final short MAGIC = (short) 0xa400;
	public static final short MAGIC_MASK = (short) 0xfc00;
	public static final int MAGIC_SHIFT = 8;
	public static final short FORCE_FLAG = 0x0200;
	public static final short RESPONSE_FLAG = 0x0100;
	public static final short NORMAL_FLAG = 0x0080;
	public static final short EXCEPTION_CODE = 0x007f;
	
	public static final short WIRE_LENGTH = 10;

	@Override
	public byte[] toWire(HeartBeatMsg msg) throws IOException {
		ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(byteStream);
		short magicFlagsStatusExCode = MAGIC;
		//System.out.println( String.format("0x%04X", magicFlagsStatusExCode));
		if (msg.isForce()) {
			magicFlagsStatusExCode |= FORCE_FLAG;
			//System.out.println( String.format("0x%04X", magicFlagsStatusExCode));
		}
		if (msg.isResponse()) {
			magicFlagsStatusExCode |= RESPONSE_FLAG;
			//System.out.println( String.format("0x%04X", magicFlagsStatusExCode));
		}
		if (msg.isNormal()) {
			magicFlagsStatusExCode |= NORMAL_FLAG;
			//System.out.println( String.format("0x%04X", magicFlagsStatusExCode));
		}
		
		magicFlagsStatusExCode |= (msg.getExCode() & EXCEPTION_CODE);
		//System.out.println( String.format("0x%04X", magicFlagsStatusExCode));
		// 写入第一个short magic Flags Status ExCode
		out.writeShort(magicFlagsStatusExCode);
		// 写入int DataNodeNumber
		out.writeInt(msg.getDataNodeNumber());
		// 写入short DataNodeGroupNumber
		out.writeShort(msg.getDataNodeGroupNumber());
		// 写入short Interval
		out.writeShort(msg.getInterval());
		out.flush();
		byte[] data = byteStream.toByteArray();
		return data;
	}

	@Override
	public HeartBeatMsg fromWire(byte[] input) throws IOException {
		if (input.length != WIRE_LENGTH) {
			throw new IOException("Wrong message size!");
		}
		DataInputStream in = new DataInputStream(
											new ByteArrayInputStream(input));
		short magic = in.readShort();
		if ((magic & MAGIC_MASK) != MAGIC) {
			throw new IOException("Bad Magic #: " +
					((magic & MAGIC_MASK) >> MAGIC_SHIFT));
		}
		boolean isForce = ((magic & FORCE_FLAG) != 0);
		boolean isResponse = ((magic & RESPONSE_FLAG) != 0);
		boolean isNormal = ((magic & NORMAL_FLAG) != 0);
		short exCode = (short) (magic & EXCEPTION_CODE);
		int dataNodeNumber = in.readInt();
		if (dataNodeNumber < 0) {
			throw new IOException("Bad DataNode Number: " + dataNodeNumber);
		}
		short dataNodeGroupNumber = in.readShort();
		if (dataNodeGroupNumber < 0) {
			throw new IOException("Bad DataNode Group Number: " + dataNodeGroupNumber);
		}
		short interval = in.readShort();
		if (interval < 0) {
			throw new IOException("Bad Interval: " + interval);
		}
		
		return new HeartBeatMsg(isForce, 
								isResponse, 
								isNormal,
								exCode,
								dataNodeNumber,
								dataNodeGroupNumber,
								interval);
	}

}
