package sa.sse.ustc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DBUtil {
	protected ResultSet rs;
	protected Connection conn;
	private final String USERNAME = "root";
	private final String PASSWORD = "y";
	private final String DRIVER = "com.mysql.jdbc.Driver";
	private final String URL = "jdbc:mysql://localhost:3306/smart?useUnicode=true&amp;characterEncoding=utf-8&useSSL=false";
	
	private Connection getConnection() throws Exception {
		Class.forName(DRIVER);
		Connection connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
		return connection;
	}
	
	protected void execSQL(String sql, Object... params) throws Exception {
		conn = getConnection();
		if (conn != null) {
			PreparedStatement pstmt = conn.prepareStatement(sql);
			for (int i=0; i<params.length; i++) {
				pstmt.setObject(i+1, params[i]);
			}
			
			if (pstmt.execute()) {
				rs = pstmt.getResultSet();
			}
		}
	}
	
	protected void closeConnection() {
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
}
