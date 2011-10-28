package DataPrepare;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedWriter;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.util.Date;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.net.ftp.FTPClient;

import MyXMLReader.MyXmlReader;

public class DataNodeWriter {

	private String hostIp;
	private String ftpHostIp;
	private int hostPort;
	private int ftpHostPort;
	private String username;
	private String password;
	private FTPClient ftpClient;
	private String[] downLoadFileList;
	private String cdr1xDir;
	private String cdrdoDir;
	private String cdrdostreamDir;
	private String dtDir;
	private String psmmDir;
    private boolean disConnect ;

	public DataNodeWriter(String path) throws IOException {
		MyXmlReader reader = new MyXmlReader(path);
		this.hostIp = reader.getName("hostip");
		this.ftpHostIp = reader.getName("ftphostip");
		this.ftpHostPort = Integer.parseInt(reader.getName("ftphostport"));
		this.hostPort = Integer.parseInt(reader.getName("hostport"));
		this.username = reader.getName("username");
		this.password = reader.getName("password");
		this.cdr1xDir = reader.getName("cdr1xtempdir");
		this.cdrdoDir = reader.getName("cdrdotempdir");
		this.cdrdostreamDir = reader.getName("cdrdostreamdir");
		this.dtDir = reader.getName("dttempdir");
		this.psmmDir = reader.getName("psmmtempdir");
		this.ftpClient = new FTPClient();
		ftpClient.setBufferSize(99999999);
		this.disConnect = false;
	}
	public void initDownLoadFileList() throws IOException {
		Socket socket = new Socket(hostIp, hostPort);
		InputStream input = socket.getInputStream();
		BufferedReader reader = new BufferedReader(new InputStreamReader(input));
		String str = null;
		String fileListArgs = "";
		while ((str = reader.readLine()) != null) {
			fileListArgs += str + "&";
		}
		downLoadFileList = fileListArgs.split("&");
	}

	public void prepareToHdfs() throws IOException, InterruptedException{
			initDownLoadFileList();
			long currentDtFileSize = 0;
			long readCdr1xSize  = 0;
			long readCdrDoSize  =0;
			long readCdrDostreamSize = 0;
			long readPsmmSize = 0;
			long readDtSize = 0;
			String currentDtFileName = null;
			String currentCdr1xFileName = null;
			String currentCdrDoFileName = null;
			String currentCdrDostreamFileName = null;
			String currentPsmmFileName = null;
			boolean dtExpectionFlag = false;
			boolean dtFlag = false;
		   
			for (int i = 0; i < downLoadFileList.length; i++) {
				try{
				ftpClient.connect(ftpHostIp, ftpHostPort);
				ftpClient.login(username, password);
				ftpClient.setControlEncoding("GBK");
				System.out.println(downLoadFileList[i]);
				ftpClient.changeWorkingDirectory(new String(downLoadFileList[i].substring(0, downLoadFileList[i].lastIndexOf("#"))));
				String[] fileArgs = downLoadFileList[i].split("/");
				String argFileName = fileArgs[fileArgs.length - 1].substring(0,fileArgs[fileArgs.length - 1].indexOf("#"));
				String argFileType = argFileName.substring(0,argFileName.lastIndexOf("_"));
				InputStream input = ftpClient.retrieveFileStream(new String(downLoadFileList[i].substring(0,downLoadFileList[i].lastIndexOf("#"))));
				BufferedReader reader = new BufferedReader(new InputStreamReader(input));
				String str = null;
				if (argFileType.equals("cdr_1x")) {
					if (currentCdr1xFileName == null)currentCdr1xFileName = cdr1xDir+ "/"+ Long.parseLong(argFileName.substring(argFileName.indexOf("2"),argFileName.indexOf("."))) / 1000000+ ".txt_1";
					if (!new String(cdr1xDir+ "/"+ Long.parseLong(argFileName.substring(argFileName.indexOf("2"),argFileName.indexOf("."))) / 1000000).equals(currentCdr1xFileName.substring(0,currentCdr1xFileName.lastIndexOf(".")))) {
						File file = new File(currentCdr1xFileName + ".temp");
						file.renameTo(new File(currentCdr1xFileName));
						currentCdr1xFileName = cdr1xDir+ "/"+ Long.parseLong(argFileName.substring(argFileName.indexOf("2"),argFileName.indexOf("."))) / 1000000+ ".txt_1";
					}
					File file = new File(currentCdr1xFileName + ".temp");
					if (file.length() >= 64000000) {
						file.renameTo(new File(currentCdr1xFileName));
						currentCdr1xFileName = currentCdr1xFileName.substring(0, currentCdr1xFileName.lastIndexOf("_") + 1)+ (Long.parseLong(currentCdr1xFileName.substring(currentCdr1xFileName.lastIndexOf("_") + 1)) + 1);
						file = new File(currentCdr1xFileName + ".temp");
					}
					if (!file.exists()) {
						file.createNewFile();
					}
					BufferedWriter writer = new BufferedWriter(new FileWriter(file, true));
					// OutputStream output = new FileOutputStream(file, true);
					// BufferedWriter writer= new BufferedWriter(new
					// OutputStreamWriter(new GZIPOutputStream(output)));
					// ////////////////////////////////////////////////////////////////////////////////////////////////////////
					if(disConnect){
					       System.out.println("=============begin continue trance===========");
					       System.out.println("read cdr1x size ="+readCdr1xSize);
				           reader.skip(readCdr1xSize);
				           disConnect = false;
					}else{
						readCdr1xSize = 0;
					}
					while ((str = reader.readLine()) != null) {
						readCdr1xSize += str.getBytes().length+2;
						if (file.length() >= 64000000) {
							writer.close();
							file.renameTo(new File(currentCdr1xFileName));
							currentCdr1xFileName = currentCdr1xFileName.substring(0, currentCdr1xFileName.lastIndexOf("_") + 1)+ (Long.parseLong(currentCdr1xFileName.substring(currentCdr1xFileName.lastIndexOf("_") + 1)) + 1);
							file = new File(currentCdr1xFileName + ".temp");
							if (!file.exists()) {
								file.createNewFile();
							}
							writer = new BufferedWriter(new FileWriter(file,true));
							// output = new FileOutputStream(file, true);
							// writer= new BufferedWriter(new
							// OutputStreamWriter(new
							// GZIPOutputStream(output)));
							// /////////////////////////////////////////////////////
						}
						writer.write(str + "\r\n");
						writer.flush();
					}
					writer.flush();
					writer.close();
				} else if (argFileType.equals("cdr_do")) {
					if (currentCdr1xFileName != null)
						new File(currentCdr1xFileName + ".temp")
								.renameTo(new File(currentCdr1xFileName));
					if (currentCdrDoFileName == null)
						currentCdrDoFileName = cdrdoDir
								+ "/"
								+ Long.parseLong(argFileName.substring(
										argFileName.indexOf("2"),
										argFileName.indexOf("."))) / 1000000
								+ ".txt_1";
					if (!new String(cdrdoDir
							+ "/"
							+ Long.parseLong(argFileName.substring(
									argFileName.indexOf("2"),
									argFileName.indexOf("."))) / 1000000)
							.equals(currentCdrDoFileName.substring(0,
									currentCdrDoFileName.lastIndexOf(".")))) {
						File file = new File(currentCdrDoFileName + ".temp");
						file.renameTo(new File(currentCdrDoFileName));
						currentCdrDoFileName = cdrdoDir
								+ "/"
								+ Long.parseLong(argFileName.substring(
										argFileName.indexOf("2"),
										argFileName.indexOf("."))) / 1000000
								+ ".txt_1";
					}
					File file = new File(currentCdrDoFileName + ".temp");
					if (file.length() >= 64000000) {
						file.renameTo(new File(currentCdrDoFileName));
						currentCdrDoFileName = currentCdrDoFileName.substring(
								0, currentCdrDoFileName.lastIndexOf("_") + 1)
								+ (Long.parseLong(currentCdrDoFileName
										.substring(currentCdrDoFileName
												.lastIndexOf("_") + 1)) + 1);
						file = new File(currentCdrDoFileName + ".temp");
					}
					if (!file.exists()) {
						file.createNewFile();
					}
					BufferedWriter writer = new BufferedWriter(new FileWriter(
							file, true));
					// OutputStream output = new FileOutputStream(file, true);
					// BufferedWriter writer= new BufferedWriter(new
					// OutputStreamWriter(new GZIPOutputStream(output)));
					// //////////////////////////////////////////////////////////////////////
					if(disConnect){
					       System.out.println("=============begin continue trance===========");
				           reader.skip(readCdrDoSize);
				           disConnect = false;
					}else{
						readCdrDoSize = 0;
					}
					while ((str = reader.readLine()) != null) {
						readCdrDoSize += str.getBytes().length+2;
						if (file.length() >= 64000000) {
							writer.close();
							file.renameTo(new File(currentCdrDoFileName));
							currentCdrDoFileName = currentCdrDoFileName
									.substring(0, currentCdrDoFileName
											.lastIndexOf("_") + 1)
									+ (Long.parseLong(currentCdrDoFileName
											.substring(currentCdrDoFileName
													.lastIndexOf("_") + 1)) + 1);
							file = new File(currentCdrDoFileName + ".temp");
							if (!file.exists()) {
								file.createNewFile();
							}
							writer = new BufferedWriter(new FileWriter(file,
									true));
							// output = new FileOutputStream(file, true);
							// writer= new BufferedWriter(new
							// OutputStreamWriter(new
							// GZIPOutputStream(output)));
							// //////////////////////////////////////////////////////
						}
						writer.write(str + "\r\n");
						writer.flush();
					}
					writer.flush();
					writer.close();
				} else if (argFileType.equals("cdr_dostream")) {
					if (currentCdrDoFileName != null)
						new File(currentCdrDoFileName + ".temp")
								.renameTo(new File(currentCdrDoFileName));
					if (currentCdrDostreamFileName == null)
						currentCdrDostreamFileName = cdrdostreamDir
								+ "/"
								+ Long.parseLong(argFileName.substring(
										argFileName.indexOf("2"),
										argFileName.indexOf("."))) / 1000000
								+ ".txt_1";
					if (!new String(cdrdostreamDir
							+ "/"
							+ Long.parseLong(argFileName.substring(
									argFileName.indexOf("2"),
									argFileName.indexOf("."))) / 1000000)
							.equals(currentCdrDostreamFileName
									.substring(0, currentCdrDostreamFileName
											.lastIndexOf(".")))) {
						File file = new File(currentCdrDostreamFileName
								+ ".temp");
						file.renameTo(new File(currentCdrDostreamFileName));
						currentCdrDostreamFileName = cdrdostreamDir
								+ "/"
								+ Long.parseLong(argFileName.substring(
										argFileName.indexOf("2"),
										argFileName.indexOf("."))) / 1000000
								+ ".txt_1";
					}
					File file = new File(currentCdrDostreamFileName + ".temp");
					if (file.length() >= 64000000) {
						file.renameTo(new File(currentCdrDostreamFileName));
						currentCdrDostreamFileName = currentCdrDostreamFileName
								.substring(0, currentCdrDostreamFileName
										.lastIndexOf("_") + 1)
								+ (Long.parseLong(currentCdrDostreamFileName
										.substring(currentCdrDostreamFileName
												.lastIndexOf("_") + 1)) + 1);
						file = new File(currentCdrDostreamFileName + ".temp");
					}
					if (!file.exists()) {
						file.createNewFile();
					}
					BufferedWriter writer = new BufferedWriter(new FileWriter(
							file, true));
					// OutputStream output = new FileOutputStream(file, true);
					// BufferedWriter writer= new BufferedWriter(new
					// OutputStreamWriter(new GZIPOutputStream(output)));
					// /////////////////////////////////////////////////////////////////////
					if(disConnect){
					       System.out.println("=============begin continue trance===========");
				           reader.skip(readCdrDostreamSize);
				           disConnect = false;
					}else{
						readCdrDostreamSize = 0;
					}
					while ((str = reader.readLine()) != null) {
						readCdrDostreamSize = str.getBytes().length+2;
						if (file.length() >= 64000000) {
							writer.close();
							file.renameTo(new File(currentCdrDostreamFileName));
							currentCdrDostreamFileName = currentCdrDostreamFileName
									.substring(0, currentCdrDostreamFileName
											.lastIndexOf("_") + 1)
									+ (Long.parseLong(currentCdrDostreamFileName
											.substring(currentCdrDostreamFileName
													.lastIndexOf("_") + 1)) + 1);
							file = new File(currentCdrDostreamFileName
									+ ".temp");
							if (!file.exists()) {
								file.createNewFile();
							}
							writer = new BufferedWriter(new FileWriter(file,
									true));
							// output = new FileOutputStream(file, true);
							// writer= new BufferedWriter(new
							// OutputStreamWriter(new
							// GZIPOutputStream(output)));
							// ////////////////////////////////////////////////////////////////////////////////////////////////////////
						}
						writer.write(str + "\r\n");
						writer.flush();
					}
					writer.flush();
					writer.close();
				} else if (argFileType.equals("psmm")) {
					if (currentCdrDostreamFileName != null)
						new File(currentCdrDostreamFileName + ".temp")
								.renameTo(new File(currentCdrDostreamFileName));
					if (currentPsmmFileName == null)
						currentPsmmFileName = psmmDir
								+ "/"
								+ Long.parseLong(argFileName.substring(
										argFileName.indexOf("2"),
										argFileName.indexOf("."))) / 1000000
								+ ".txt_1";
					if (!new String(psmmDir
							+ "/"
							+ Long.parseLong(argFileName.substring(
									argFileName.indexOf("2"),
									argFileName.indexOf("."))) / 1000000)
							.equals(currentPsmmFileName.substring(0,
									currentPsmmFileName.lastIndexOf(".")))) {
						File file = new File(currentPsmmFileName + ".temp");
						file.renameTo(new File(currentPsmmFileName));
						currentPsmmFileName = psmmDir
								+ "/"
								+ Long.parseLong(argFileName.substring(
										argFileName.indexOf("2"),
										argFileName.indexOf("."))) / 1000000
								+ ".txt_1";
					}
					File file = new File(currentPsmmFileName + ".temp");
					if (file.length() >= 64000000) {
						file.renameTo(new File(currentPsmmFileName));
						currentPsmmFileName = currentPsmmFileName.substring(0,
								currentPsmmFileName.lastIndexOf("_") + 1)
								+ (Long.parseLong(currentPsmmFileName
										.substring(currentPsmmFileName
												.lastIndexOf("_") + 1)) + 1);
						file = new File(currentPsmmFileName + ".temp");
					}
					if (!file.exists()) {
						file.createNewFile();
					}
					BufferedWriter writer = new BufferedWriter(new FileWriter(
							file, true));
					// OutputStream output = new FileOutputStream(file, true);
					// BufferedWriter writer= new BufferedWriter(new
					// OutputStreamWriter(new GZIPOutputStream(output)));
					// //////////////////////////////////////////////////////////////////////
					if(disConnect){
					       System.out.println("=============begin continue trance===========");
				           reader.skip(readPsmmSize);
				           disConnect = false;
					}else{
						readPsmmSize = 0;
					}
					
					while ((str = reader.readLine()) != null) {
						readPsmmSize += str.getBytes().length+2;
						if (file.length() >= 64000000) {
							writer.close();
							file.renameTo(new File(currentPsmmFileName));
							currentPsmmFileName = currentPsmmFileName.substring(0, currentPsmmFileName.lastIndexOf("_") + 1)+ (Long.parseLong(currentPsmmFileName.substring(currentPsmmFileName.lastIndexOf("_") + 1)) + 1);
							file = new File(currentPsmmFileName + ".temp");
							if (!file.exists()) {
								file.createNewFile();
							}
							writer = new BufferedWriter(new FileWriter(file,
									true));
							// output = new FileOutputStream(file, true);
							// writer= new BufferedWriter(new
							// OutputStreamWriter(new
							// GZIPOutputStream(output)));
							// ////////////////////////////////////////////////////
						}
						writer.write(str + "\r\n");
						writer.flush();
					}
					writer.flush();
					writer.close();
				} else {
					if (currentPsmmFileName != null)new File(currentPsmmFileName + ".temp").renameTo(new File(currentPsmmFileName));
					if(disConnect){
					       System.out.println("=============begin continue trance===========");
				           reader.skip(readDtSize);
				           disConnect = false;
					}else{
						 readDtSize = 0;
						 if(currentDtFileName != null){
							 File file = new File(currentDtFileName+".temp");
							 file.renameTo(new File(currentDtFileName));
						 }					 
						currentDtFileName = dtDir	+ "/" + argFileName.substring(argFileName.lastIndexOf("_") + 1,argFileName.lastIndexOf(".")) + "_"+i+ ".txt_1";
					}			
					File file = new File(currentDtFileName + ".temp");
					if (!file.exists()) {
						file.createNewFile();
					}
					BufferedWriter writer = new BufferedWriter(new FileWriter(file, true));
					while((str=reader.readLine())!=null){
						readDtSize += str.getBytes().length+2;
						if (file.length() >= 64000000) {
							writer.close();
							file.renameTo(new File(currentDtFileName));
							currentDtFileName = currentDtFileName.substring(0, currentDtFileName.lastIndexOf("_") + 1)+ (Long.parseLong(currentDtFileName.substring(currentDtFileName.lastIndexOf("_") + 1)) + 1);
							file = new File(currentDtFileName + ".temp");
							if (!file.exists()) {
								file.createNewFile();
							}
							writer = new BufferedWriter(new FileWriter(file,true));
						}
						writer.write(str + "\r\n");
						writer.flush();
					}
					writer.flush();
					writer.close();
					
				}
				ftpClient.disconnect();
				reader.close();
				}catch (Exception e) {
					System.out.println("ftp server error diconnect to the server ");
					Thread.sleep(10000);
				    disConnect = true ;
				    i -=2 ;
				}
			}
			if (currentCdr1xFileName != null)	new File(currentCdr1xFileName + ".temp").renameTo(new File(currentCdr1xFileName));
			if (currentDtFileName != null) new File(currentDtFileName + ".temp").renameTo(new File(currentDtFileName));
			if (currentCdrDoFileName != null)	new File(currentCdrDoFileName + ".temp").renameTo(new File(currentCdrDoFileName));
			if (currentPsmmFileName != null)	new File(currentPsmmFileName + ".temp").renameTo(new File(currentPsmmFileName));
			if (currentCdrDostreamFileName != null)	new File(currentCdrDostreamFileName + ".temp").renameTo(new File(currentCdrDostreamFileName));
	}

	public static void main(String[] args) throws Exception {
		System.out.println(new Date());
		DataNodeWriter writer = new DataNodeWriter(
				"E:/JavaWorkspace/Telecom/conf/DataNodeWriterConf.xml");
		writer.prepareToHdfs();
		System.out.println(new Date());
	}
}
