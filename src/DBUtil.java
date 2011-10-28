import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DBUtil {
	private static String dbUrl;
	private static String dbDriver;
	private static String dbUser;
	private static String dbPassword;
	static {
        Properties prop = new Properties();
        String dir = DBUtil.class.getClassLoader().getResource("").getPath();
        try {
        	FileInputStream  inputFile = new FileInputStream(dir+"db.properties");
            prop.load(inputFile);
            dbUrl = prop.getProperty("dbUrl").trim();
            dbDriver = prop.getProperty("dbDriver").trim();
            dbUser = prop.getProperty("dbUser").trim();
            dbPassword = prop.getProperty("dbPassword").trim();
            
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
	public static Connection  getConnection(){
		Connection conn = null ;
		try {
			Class.forName(dbDriver);
			//String url = dbUrl+"?user="+dbUser+"&password="+dbPassword+"&useUnicode=true&characterEncoding=GBK";
			conn = DriverManager.getConnection(dbUrl,dbUser,dbPassword);
			//conn = DriverManager.getConnection(url);
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return conn;
	}
}
