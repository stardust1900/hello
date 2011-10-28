package createIndex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.zookeeper.KeeperException;

import zookeeperDispatch.CloudComputingServer.CloudComputingServer;
import MyXMLReader.MyXmlReader;

public class ReadFromHDFSSever  extends  CloudComputingServer {

	
    FileSystem fs;
    long endTime = 0;
    String parentName ="";

    long  hdfsFileNumber = 0;
    private String subCut="#";
    private String cut = "@";
    private String hdfsURL ="";
        
    private String orgRequestContent="";
    private String fileName="";
    private  String repairFileName="";
	
	public ReadFromHDFSSever(String fileName ,String repairFileName)
	{
		super(fileName);
		this.fileName = fileName;
		this.repairFileName = repairFileName;
		initConf(fileName);
		Configuration conf = new Configuration(); 
    	conf.set("fs.default.name",hdfsURL);
   	try {
			fs = FileSystem.get(conf);
		} catch (IOException e) { 
			e.printStackTrace();
		}
	}
	
		
    private void initConf(String confFileName)
    {
    	MyXmlReader reader = new MyXmlReader(confFileName);
    		hdfsURL= reader.getName("hdfsURL");
    		parentName = reader.getName("sourceRoot");
    }
	
    
    public void parseAttribute()
    {
    	endTime 				= Long.parseLong(getNextAttibute());
    	hdfsFileNumber = Long.parseLong(getNextAttibute());
    	super.parseAttribute();
    	PrintAttribute();
    }
    
    public void PrintAttribute()
    {
    	System.out.println("endTime  is " + endTime);
    	System.out.println("hdfsFileNumber  is " + hdfsFileNumber);
    	super.PrintAttribute();
    }
    public  void saveAttribute()
    {
    	clearAttribute();
		this.addSaveAttribute(endTime+"");
		this.addSaveAttribute(hdfsFileNumber+"");
    	super.saveAttribute();
    }
    
    public  String  EncapsulationRequest()
	{
		 //format:path@ID@path@ID@path@ID     write for the client , save for sever change recover the file and ID;
		return  orgRequestContent;
	}
	
    
    public void beforeSendData()
    {
    	 //format:path@ID@path@ID@path@ID     write for the client , save for sever change recover the file and ID;
    	
    }
    
	
	//��master��Ҫ������
	public void doAction() 
	{
		RepairThread repair = new RepairThread(fileName);
		repair.setCreateIndexConf(repairFileName);
		repair.setFileSystem(fs);
		Thread thread = new Thread(repair);
		//thread.start();
		
    	while(true)
    	{	
    		System.out.println("��ʼ��ѯ");
    		CheckNewFile();
    		try {
				Thread.sleep(20000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
    	}
	}
	
	
	 public void CheckNewFile( )
	    {
	    	 List<String>  list = GetFileNames();
	    		try {
	    			
	    		    //format:path@ID@path@ID@path@ID     write for the client , save for sever change recover the file and ID;
	    			orgRequestContent ="";
	    			for( int m = 0 ; m < list.size()  ; m++)
	    			{
	    				String nodePath = list.get(m);
	    				hdfsFileNumber++;
	    				orgRequestContent = orgRequestContent+nodePath+cut+hdfsFileNumber+cut;
	    			}
	    			if(orgRequestContent.length()  > 0)
	    			{
		    			orgRequestContent = orgRequestContent.substring(0,orgRequestContent.length()-1);
		    			sendData(EncapsulationRequest());
	    			}
				} catch (Exception e) {
					e.printStackTrace();
				}
	    }
	 
	 
	    public String GetParentName()
	    {	
	    	return parentName;
	    }
	    private  List<String> GetFileNames( )
	    {
	    	String parentName = GetParentName();
	    	ArrayList<String> fileNameList = new ArrayList<String>();
	    	 try {
	    		 System.out.println(parentName+"******************************");
	    		 if( fs  != null)
	    		 {
					FileStatus fileList[] = fs.listStatus(new Path(parentName));
					if(fileList  != null)
					{
									int size = fileList.length;
									long onceMaxTime = 0;
									long cdrEndTime =endTime;
								
									for (int i = 0; i < size; i++) {
										if (onceMaxTime < fileList[i].getModificationTime()) {
											onceMaxTime = fileList[i].getModificationTime();
										}
				
										if (fileList[i].getModificationTime() > cdrEndTime) {
											fileNameList.add(fileList[i].getPath().getName());
										}
									}
									endTime = onceMaxTime;
						}
				}
	    	 } catch (IOException e) {
	 			e.printStackTrace();
	 		}
	    	return fileNameList;
	    }
	     
	public static void main(String[] args)
	{
		String confile =args[0];
		String repairFile=args[1];
		ReadFromHDFSSever  test = new ReadFromHDFSSever(confile,repairFile);
		try {
			test.check();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (KeeperException e) {
			e.printStackTrace();
		}
	}
}
