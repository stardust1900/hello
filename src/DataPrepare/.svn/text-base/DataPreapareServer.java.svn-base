package DataPrepare;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;

import org.apache.commons.net.ftp.FTPClient;

public class DataPreapareServer implements Runnable{
	
	private FTPClient ftpClient;
	private String readBuffer;
	private void init(String host,int port,String user,String password) throws SocketException, IOException {
		ftpClient = new FTPClient();
		ftpClient.connect(host, port);
		ftpClient.login(user, password);
	}
	
	public void run() {
		try {
			ServerSocket serverSocket = new ServerSocket(5000);
			OutputStream output = null;
			BufferedWriter writer = null;
			while(true){
				Socket socket = serverSocket.accept();
				output = socket.getOutputStream();
				writer =new BufferedWriter(new OutputStreamWriter(output));
				writer.write(readBuffer);
				writer.newLine();
				writer.flush();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public static void main(String[] args) {
		DataPreapareServer server = new DataPreapareServer();
		Thread serverThread = new Thread(server);
		serverThread.start();
	}
	
}
