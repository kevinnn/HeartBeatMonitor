package sa.sse.ustc;

import java.io.IOException;

public interface HeartBeatMsgCoder {
	byte[] toWire(HeartBeatMsg msg) throws IOException;
	HeartBeatMsg fromWire(byte[] input) throws IOException;
}
