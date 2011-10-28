import java.io.IOException;
import java.net.SocketException;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;


public class FtpTest {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws SocketException 
	 */
	public static void main(String[] args) throws SocketException, IOException {
		FTPClient ftpClient = new FTPClient();
		ftpClient.connect("192.168.0.94", 21);
		ftpClient.login("cstor", "cstor");
		System.out.println("=============");
		System.out.println(ftpClient.DEFAULT_CONTROL_ENCODING);

		for(FTPFile f : ftpClient.listFiles("DT/")){
			System.out.println(new String(f.getName().getBytes("ISO-8859-1"),"GBK"));
		}
		//ftpClient.
//		for(String str : ftpClient.listNames()){
//			System.out.println(str);
//		}
	}

}
