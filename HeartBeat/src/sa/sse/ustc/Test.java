package sa.sse.ustc;

import static org.junit.Assert.fail;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;

public class Test {

	@org.junit.Test
	public void test() {
		fail("Not yet implemented");
	}
	
	@Before
	public void init(){
		
	}
	
	@After
	public void destroy(){
		
	}
	
	// 测试HeartBeatMsgBinCoder
	@org.junit.Test
	public void testHeartBeatMsgBinCoder(){
		//HeartBeatMsg in = new HeartBeatMsg(false,false,true,(short)55, 1, (short) 2, (short)5);
		HeartBeatMsg in = new HeartBeatMsg(true,true,true,(short)55, 1, (short) 2, (short)5);
		System.out.println(in.toString());
		byte[] out = null;
		try {
			out = new HeartBeatMsgBinCoder().toWire(in);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			HeartBeatMsg result = new HeartBeatMsgBinCoder().fromWire(out);
			System.out.println(result.toString());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
