package createIndex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.zookeeper.KeeperException;

import zookeeperDispatch.CloudComputingClient.CloudComputingClient;
import MyXMLReader.MyXmlReader;
import Tools.DebugPrint;

public class ReadFromHDFSClient  extends CloudComputingClient{
	
    FileSystem fs;
    long endTime = 0;

    long  hdfsFileNumber = 0;
    
    private String subCut="#";
    private String cut = "@";
    private String hdfsURL ="";
    private String  indexConfile  ="";
    
    private ArrayList<CreateHDFSIndex>  createIndexList = new ArrayList<CreateHDFSIndex>();
    int threadNumber = 7;
    private BlockingQueue<Pair<String,String>> queue = new  ArrayBlockingQueue<Pair<String,String>>(1024*10);
	
	public ReadFromHDFSClient(String conFileName , String  lindexConfile)
	{
		super(conFileName);
		indexConfile = lindexConfile;
		initConf(conFileName);
		Configuration conf = new Configuration();
		conf.set("fs.default.name", hdfsURL );
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
		for(int i  = 0 ; i  < threadNumber ; i++ )
		{
			  CreateHDFSIndex index  = new CreateHDFSIndex(queue,this,indexConfile,fs);
			  createIndexList.add(index);
			  Thread thread = new Thread( index);
			  thread.start();
		}

	}
	
    private void initConf(String confFileName)
    {
    	MyXmlReader reader = new MyXmlReader(confFileName);
		hdfsURL= reader.getName("hdfsURL");
		threadNumber = Integer.parseInt(reader.getName("createIndexThreadNumber"));
    }
    
	private  void  parseRequest()
	{
		 //format:path@ID@path@ID@path@ID     write for the client , save for sever change recover the file and ID;
		
	}
	
	//Client��Ҫ������
	  public void doAction(String RequestContent,String nodeName)
	{
		  queue.add(new Pair(RequestContent,nodeName));
		  DebugPrint.DebugPrint("serverSendRequestIs " + RequestContent);		  
		  System.out.println("RequestContent  is " + RequestContent +"   nodeName  is " + nodeName );
	}
	  
		public static void main(String[] args)
		{
			String confile =args[0];
			String indexConfile =args[1];
			ReadFromHDFSClient  test = new ReadFromHDFSClient(confile ,indexConfile );
			try {
				test.check();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (KeeperException e) {
				e.printStackTrace();
			}
		}

}
