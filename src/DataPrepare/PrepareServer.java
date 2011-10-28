package DataPrepare;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import MyXMLReader.MyXmlReader;


public class PrepareServer{

	private FTPClient ftpClient;
	private String host;
	private int port;
	private String username;
	private String password;
    private String cdrUrl;
	private String psmmUrl;
	private String dtUrl;
	private List<String> fileCatalogs;
	private Map<String, Long> fileList;
	private Long filesAllSize;
	private List<String> cdr_1xFileList;
    private List<String> cdr_doFileList;
    private List<String> cdr_dostreamFileList;
    private List<String> psmmFileList;
    private List<String> dtFileList;
    private Map<Long,StringBuffer> cdr_1xResult;
    private Map<Long,StringBuffer> cdr_doResult;
    private Map<Long,StringBuffer> cdr_dostreamResult;
    private Map<Long,StringBuffer> psmmResult;
    
	public PrepareServer(String path) throws SocketException, IOException {
		MyXmlReader reader = new MyXmlReader(path);
		cdr_1xFileList = new ArrayList<String>();
		cdr_doFileList = new ArrayList<String>();
		cdr_dostreamFileList = new ArrayList<String>();
		psmmFileList = new ArrayList<String>();
		dtFileList = new ArrayList<String>();
		cdr_1xResult = new HashMap<Long, StringBuffer>();
		cdr_doResult = new HashMap<Long, StringBuffer>();
		cdr_dostreamResult = new HashMap<Long, StringBuffer>();
		psmmResult = new HashMap<Long, StringBuffer>();
		this.host = reader.getName("host");
		this.port = Integer.parseInt(reader.getName("port"));
		this.username = reader.getName("username");
		this.password = reader.getName("password");
        this.cdrUrl = reader.getName("cdrurl");
		this.psmmUrl = reader.getName("psmmurl");
		this.dtUrl = reader.getName("dturl");
		ftpClient = new FTPClient();
		ftpClient.connect(host, port);
		ftpClient.login(username, password);
		//ftpClient.setControlEncoding(" ");
		fileList = new HashMap<String, Long>();
		fileCatalogs = new ArrayList<String>();
		if(!cdrUrl.equals("null")){
		String[] cdrUrls = cdrUrl.split(",");
		for(int i=0;i<cdrUrls.length;i++){
			fileCatalogs.add(cdrUrls[i]);
		}
		}
		if(!psmmUrl.equals("null")){
		String[] psmmUrls = psmmUrl.split(",");
		for(int i=0;i<psmmUrls.length;i++){
			fileCatalogs.add(psmmUrls[i]);
		}
		}
		if(!dtUrl.equals("null")) fileCatalogs.add(dtUrl);
		this.filesAllSize = 0l;
	}

	public void listFilesForPrepare() throws IOException {
		for (String fileCatalog : fileCatalogs) {
			ftpClient.changeWorkingDirectory(fileCatalog);
			FTPFile[] ftpFiles = ftpClient.listFiles();
			for (int i = 2; i < ftpFiles.length; i++) {
				String fileName = new String(ftpFiles[i].getName().getBytes("iso-8859-1"),"GBK");
				Long size = ftpFiles[i].getSize();
				String filePath = fileCatalog + "/" + fileName;
				this.filesAllSize += size;
				fileList.put(filePath, size);
			}
			   System.out.println("Scanning file catalog :"+fileCatalog+" finish \nfiles="+ftpFiles.length+"current all size="+filesAllSize);		
		}
	}
	public void catalaFilter() throws IOException{
		listFilesForPrepare();
		for(String key : fileList.keySet() ){
			String[] fileArgs = key.split("/");
			String argFileName = fileArgs[fileArgs.length-1];
			String argFileType = argFileName.substring(0,argFileName.lastIndexOf("_"));
			boolean flag = true;
			if(argFileType.equals("cdr_1x")){
				Long argFileTime = Long.parseLong(argFileName.substring(argFileName.indexOf("2"),argFileName.indexOf(".")));
				if(cdr_1xFileList.size() == 0){
					cdr_1xFileList.add(key+"#"+fileList.get(key));
				}else{
				    for(int i=0;i<cdr_1xFileList.size();i++){
			            String[] cdr_1xFileArgs=  cdr_1xFileList.get(i).split("/");
			            Long  cdr_1xTime = Long.parseLong(cdr_1xFileArgs[3].substring(cdr_1xFileArgs[3].indexOf("2"),cdr_1xFileArgs[3].lastIndexOf(".")));
				        if(cdr_1xTime >=  argFileTime){
				        	cdr_1xFileList.add(i, key+"#"+fileList.get(key));
				        	flag = false;
				        	break;
				        }    
				    }if(flag){
				        cdr_1xFileList.add(key+"#"+fileList.get(key));
				    }
				} 
			}else if(argFileType.equals("cdr_do")){
				Long argFileTime = Long.parseLong(argFileName.substring(argFileName.indexOf("2"),argFileName.indexOf(".")));
				if(cdr_doFileList.size() == 0){
					cdr_doFileList.add(key+"#"+fileList.get(key));
				}else{
				    for(int i=0;i<cdr_doFileList.size();i++){
			            String[] cdr_doFileArgs=  cdr_doFileList.get(i).split("/");
			            Long  cdr_doTime = Long.parseLong(cdr_doFileArgs[3].substring(cdr_doFileArgs[3].indexOf("2"),cdr_doFileArgs[3].lastIndexOf(".")));
				        if(cdr_doTime >=  argFileTime){
				        	cdr_doFileList.add(i, key+"#"+fileList.get(key));
				        	flag = false;
				        	break;
				        }    
				    }if(flag){
				        cdr_doFileList.add(key+"#"+fileList.get(key));
				    }
				} 
			
			}else if(argFileType.equals("cdr_dostream")){
				Long argFileTime = Long.parseLong(argFileName.substring(argFileName.indexOf("2"),argFileName.indexOf(".")));
				if(cdr_dostreamFileList.size() == 0){
					cdr_dostreamFileList.add(key+"#"+fileList.get(key));
				}else{
				    for(int i=0;i<cdr_dostreamFileList.size();i++){
			            String[] cdr_dostreamFileArgs=  cdr_dostreamFileList.get(i).split("/");
			            Long  cdr_dostreamTime = Long.parseLong(cdr_dostreamFileArgs[3].substring(cdr_dostreamFileArgs[3].indexOf("2"),cdr_dostreamFileArgs[3].lastIndexOf(".")));
				        if(cdr_dostreamTime >=  argFileTime){
				        	cdr_dostreamFileList.add(i, key+"#"+fileList.get(key));
				        	flag = false;
				        	break;
				        }    
				    }if(flag){
				        cdr_dostreamFileList.add(key+"#"+fileList.get(key));
				    }
				} 
			
			}else if(argFileType.equals("psmm")){
				Long argFileTime = Long.parseLong(argFileName.substring(argFileName.indexOf("2"),argFileName.indexOf(".")));
				if(psmmFileList.size() == 0){
					psmmFileList.add(key+"#"+fileList.get(key));
				}else{
				    for(int i=0;i<psmmFileList.size();i++){
			            String[] psmmFileArgs=  psmmFileList.get(i).split("/");
			            Long  psmmTime = Long.parseLong(psmmFileArgs[3].substring(psmmFileArgs[3].indexOf("2"),psmmFileArgs[3].lastIndexOf(".")));
				        if(psmmTime >=  argFileTime){
				        	psmmFileList.add(i, key+"#"+fileList.get(key));
				        	flag = false;
				        	break;
				        }    
				    }if(flag){
				        psmmFileList.add(key+"#"+fileList.get(key));
				    }
				} 
			}else{
				dtFileList.add(key+"#"+fileList.get(key));
			}
		}
		System.out.println("cdr_1x :"+cdr_1xFileList.size()+" cdr_do:"+cdr_doFileList.size()+" cdr_dostream:"+cdr_dostreamFileList.size()+" psmm:"+psmmFileList.size()+" dt:"+dtFileList.size());
	}
	
	public void  sendForPrepare() throws IOException{
		catalaFilter();
		String[] fileArgs = cdr_1xFileList.get(0).split("/");
		String argFileName = fileArgs[fileArgs.length-1];
		String argFileType = argFileName.substring(0,argFileName.lastIndexOf("_"));
		Long argFileTime = Long.parseLong(argFileName.substring(argFileName.indexOf("2"),argFileName.indexOf(".")));
		Long currentArgFileTimeDate = argFileTime/1000000;
		cdr_1xResult.put(currentArgFileTimeDate, new StringBuffer(cdr_1xFileList.get(0)));
		for(int i=1;i<cdr_1xFileList.size();i++){
			fileArgs = cdr_1xFileList.get(i).split("/");
			argFileName = fileArgs[fileArgs.length-1];
			argFileType = argFileName.substring(0,argFileName.lastIndexOf("_"));
			argFileTime = Long.parseLong(argFileName.substring(argFileName.indexOf("2"),argFileName.indexOf(".")));
			Long argFileTimeDate = argFileTime/1000000;
			if(currentArgFileTimeDate.equals(argFileTimeDate)){
				cdr_1xResult.get(currentArgFileTimeDate).append("&"+cdr_1xFileList.get(i));
			}else{
				currentArgFileTimeDate = argFileTimeDate;
				cdr_1xResult.put(currentArgFileTimeDate, new StringBuffer(cdr_1xFileList.get(i)));
			}
		}
		for(Long key : cdr_1xResult.keySet()){
			System.out.println(cdr_1xResult.get(key));
		}
		
		fileArgs = cdr_doFileList.get(0).split("/");
		argFileName = fileArgs[fileArgs.length-1];
		argFileType = argFileName.substring(0,argFileName.lastIndexOf("_"));
	    argFileTime = Long.parseLong(argFileName.substring(argFileName.indexOf("2"),argFileName.indexOf(".")));
		currentArgFileTimeDate = argFileTime/1000000;
		cdr_doResult.put(currentArgFileTimeDate, new StringBuffer(cdr_doFileList.get(0)));
		for(int i=1;i<cdr_doFileList.size();i++){
			fileArgs = cdr_doFileList.get(i).split("/");
			argFileName = fileArgs[fileArgs.length-1];
			argFileType = argFileName.substring(0,argFileName.lastIndexOf("_"));
			argFileTime = Long.parseLong(argFileName.substring(argFileName.indexOf("2"),argFileName.indexOf(".")));
			Long argFileTimeDate = argFileTime/1000000;
			if(currentArgFileTimeDate.equals(argFileTimeDate)){
				cdr_doResult.get(currentArgFileTimeDate).append("&"+cdr_doFileList.get(i));
			}else{
				currentArgFileTimeDate = argFileTimeDate;
				cdr_doResult.put(currentArgFileTimeDate, new StringBuffer(cdr_doFileList.get(i)));
			}
		}
		for(Long key : cdr_doResult.keySet()){
			System.out.println(cdr_doResult.get(key));
		}
		
		fileArgs = cdr_dostreamFileList.get(0).split("/");
		argFileName = fileArgs[fileArgs.length-1];
		argFileType = argFileName.substring(0,argFileName.lastIndexOf("_"));
		argFileTime = Long.parseLong(argFileName.substring(argFileName.indexOf("2"),argFileName.indexOf(".")));
		currentArgFileTimeDate = argFileTime/1000000;
		cdr_dostreamResult.put(currentArgFileTimeDate, new StringBuffer(cdr_dostreamFileList.get(0)));
		for(int i=1;i<cdr_dostreamFileList.size();i++){
			fileArgs = cdr_dostreamFileList.get(i).split("/");
			argFileName = fileArgs[fileArgs.length-1];
			argFileType = argFileName.substring(0,argFileName.lastIndexOf("_"));
			argFileTime = Long.parseLong(argFileName.substring(argFileName.indexOf("2"),argFileName.indexOf(".")));
			Long argFileTimeDate = argFileTime/1000000;
			if(currentArgFileTimeDate.equals(argFileTimeDate)){
				cdr_dostreamResult.get(currentArgFileTimeDate).append("&"+cdr_dostreamFileList.get(i));
			}else{
				currentArgFileTimeDate = argFileTimeDate;
				cdr_dostreamResult.put(currentArgFileTimeDate, new StringBuffer(cdr_dostreamFileList.get(i)));
			}
		}
		for(Long key : cdr_dostreamResult.keySet()){
			System.out.println(cdr_dostreamResult.get(key));
		}
		
		fileArgs = psmmFileList.get(0).split("/");
		argFileName = fileArgs[fileArgs.length-1];
		argFileType = argFileName.substring(0,argFileName.lastIndexOf("_"));
		argFileTime = Long.parseLong(argFileName.substring(argFileName.indexOf("2"),argFileName.indexOf(".")));
		currentArgFileTimeDate = argFileTime/1000000;
		psmmResult.put(currentArgFileTimeDate, new StringBuffer(psmmFileList.get(0)));
		for(int i=1;i<psmmFileList.size();i++){
			fileArgs = psmmFileList.get(i).split("/");
			argFileName = fileArgs[fileArgs.length-1];
			argFileType = argFileName.substring(0,argFileName.lastIndexOf("_"));
			argFileTime = Long.parseLong(argFileName.substring(argFileName.indexOf("2"),argFileName.indexOf(".")));
			Long argFileTimeDate = argFileTime/1000000;
			if(currentArgFileTimeDate.equals(argFileTimeDate)){
				psmmResult.get(currentArgFileTimeDate).append("&"+psmmFileList.get(i));
			}else{
				currentArgFileTimeDate = argFileTimeDate;
				psmmResult.put(currentArgFileTimeDate, new StringBuffer(psmmFileList.get(i)));
			}
		}
		for(Long key : psmmResult.keySet()){
			System.out.println(psmmResult.get(key));
		}
	}

	public void beginToSend() throws IOException{
		List <StringBuffer> result = new ArrayList<StringBuffer>();
		ServerSocket serverSocket = new ServerSocket(5000);
        List<BufferedWriter> writers = new ArrayList<BufferedWriter>();
        while(true){
        	 Socket socket = serverSocket.accept();
        	 socket.setKeepAlive(true);
         	 BufferedWriter output= new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
             writers.add(output);
             if(writers.size() >= 5){
            	 break;
             }
          }
        for(int i=0;i<writers.size();i++){
        	writers.get(i).write(result.get(i).toString());
        	writers.get(i).newLine();
        	writers.get(i).flush();
        	writers.get(i).close();
        }
	}
	
	public void sendFileList() throws IOException{
		sendForPrepare();
		List<StringBuffer> result = new ArrayList<StringBuffer>();
		for(Long key : cdr_1xResult.keySet()){
			result.add(cdr_1xResult.get(key));
		}
		for(Long key : cdr_doResult.keySet()){
			result.add(cdr_doResult.get(key));
		}
		for(Long key : cdr_dostreamResult.keySet()){
			result.add(cdr_dostreamResult.get(key));
		}
		for(Long key : psmmResult.keySet()){
			result.add(psmmResult.get(key));
		}
		
		List<BufferedWriter> writers = new ArrayList<BufferedWriter>();
		System.out.println("client init");
		ServerSocket serverSocket = new ServerSocket(5000);
		boolean flag = true;
		while(flag){
			Socket socket = serverSocket.accept();
			socket.setKeepAlive(true);
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
			writers.add(writer);
			if(writers.size() >=1){  //5
				flag = false;
			}
		}
		int writerIndex = 0;//4
		for(int i=0;i<result.size();i++){
             writers.get(writerIndex).write(result.get(i).toString());
             writers.get(writerIndex).newLine();
             writers.get(writerIndex).flush();
             writerIndex --;
             if(writerIndex==-1){
            	 writerIndex = 0;//4
             }
		}
		writerIndex = 0;
	    for(int i=0 ;i<dtFileList.size();i++){
	    	 writers.get(writerIndex).write(dtFileList.get(i).toString());
             writers.get(writerIndex).newLine();
             writers.get(writerIndex).flush();
             writerIndex ++;
             if(writerIndex==1){//5
            	 writerIndex = 0;
             }
	    }
		for(int i=0;i<writers.size();i++){
			writers.get(i).close();
		}
		ftpClient.disconnect();
	}
	
	public static void main(String[] args) throws SocketException, IOException {
		PrepareServer server = new PrepareServer("E:/JavaWorkspace/Telecom/conf/PrepareServerConf.xml");
	    server.sendFileList();
	}
}
