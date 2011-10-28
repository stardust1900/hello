package DataPrepare;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.Socket;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import MyXMLReader.MyXmlReader;

public class DataInputClient{

	private Socket socket;
	private String serverAddr;
	private int cdrMergeRowsCount;
	private int cdrRowsCount;
	private String cdrPath;
	private Configuration conf;
	private FileSystem fs;
	private String hdfsUri;
	private FSDataOutputStream writer;
	
	public DataInputClient(String path) throws Exception {
		MyXmlReader reader = new MyXmlReader(path);
		serverAddr = reader.getName("serveraddr");
		cdrMergeRowsCount = Integer.parseInt(reader.getName("cdrmergerowscount"));
		hdfsUri = reader.getName("hdfsuri");
		cdrRowsCount = -1;
		socket = new Socket(serverAddr, 5000);
	    conf = new Configuration();
	    fs = FileSystem.get(URI.create(hdfsUri), conf);
	    cdrPath = reader.getName("cdrpath");
	}
	
	public void receiveDataFromServer() throws Exception{
	      InputStream input = socket.getInputStream();	
	      BufferedReader reader = new BufferedReader(new InputStreamReader(input));
	      String str = null;
	      while((str = reader.readLine())!=null){
	    	  if(cdrRowsCount == -1 || cdrRowsCount == cdrMergeRowsCount){
	    		  writer = fs.create(new Path(cdrPath+"cdr_"+System.currentTimeMillis()));
	    		  if(cdrRowsCount == -1){
	    			  cdrRowsCount ++;
	    		  }else{
	    			  cdrRowsCount = 0;
	    		  }
	    	  }
	    	  writer.write((str+"\r\n").getBytes());
	    	  writer.flush();
	      }
	      writer.close();
	}
	
	public static void main(String[]args){
		
	}

}
