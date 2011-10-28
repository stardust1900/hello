package DataPrepare;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.Socket;
import java.net.URI;
import java.net.UnknownHostException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import MyXMLReader.MyXmlReader;

public class CDRPrepareClient implements Runnable{

	private String cdr_1xHdfsDir;
	private String cdr_doHdfsDir;
	private String psmmHdfsDir;
	private String hdfsUri;
	private String serverAddr;
	private Configuration configuration;
	private FileSystem fileSystem;
	private static String confPath;

	public void init(String path) throws IOException{
		MyXmlReader xmlReader = new MyXmlReader(path);
		this.cdr_1xHdfsDir = xmlReader.getName("cdr1xhdfsdir");
		this.cdr_doHdfsDir = xmlReader.getName("cdr_dohdfsDir");
		this.psmmHdfsDir = xmlReader.getName("psmmhdfsdir");
		this.confPath = path;
		this.hdfsUri = xmlReader.getName("hdfsuri");
		this.serverAddr = xmlReader.getName("serveraddr");
		configuration = new Configuration();
		fileSystem = FileSystem.get(URI.create(hdfsUri),configuration);
	}
	public CDRPrepareClient(String path) throws IOException {
		init(path);
	}

	public  void writeToHdfs(String inputData, int dataType,String curentFileName) throws IOException {
		// 1---cdr_1x
		// 2---cdr_do
		// 3---psmm
		FSDataOutputStream writer = null;
		switch (dataType) {
		case 1:
			writer = fileSystem.append(new Path(cdr_1xHdfsDir));
			break;
		case 2:
			writer = fileSystem.append(new Path(psmmHdfsDir));
			break;
		case 3:
			writer = fileSystem.append(new Path(cdr_doHdfsDir));
			break;
		}
		writer.write((inputData+"\r\n").getBytes());
		writer.flush();
		writer.close();
	}
	@Override
	public void run() {
		try {
			init(CDRPrepareClient.confPath);	
			Socket socket = new Socket(serverAddr,5000);
			InputStream  input;
			BufferedReader reader;
			while(socket.isConnected()){
			    input = socket.getInputStream();
			    reader = new BufferedReader(new InputStreamReader(input));
			    String str = reader.readLine();
			    String[] args = str.split("#");
			    writeToHdfs(args[0] ,Integer.parseInt(args[1]) , args[2]);
			}
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public static void main(String[] args) throws IOException {
		CDRPrepareClient client = new CDRPrepareClient(args[0]);
		Thread clientThread = new Thread(client);
		clientThread.start();
	}
}
