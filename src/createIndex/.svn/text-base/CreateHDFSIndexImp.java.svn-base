package createIndex;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Pair;

import Data.CDRTable;
import ParseData.ParseData;
import Tools.DebugPrint;


public class CreateHDFSIndexImp {
	
	 ParseData pdbssap =null;
	 String logFile="";
	 PrintWriter  myFile =  null;
	 String queryPath="";
	 FileSystem fs=null;
	 
	 ArrayList<Pair<String,Long>>list= null;
	 
	 public  CreateHDFSIndexImp()
	 {
			 pdbssap = new generateIndex();
	 }
	 
	 public void setLogFile(String llogFile)
	 {
		 	logFile =llogFile;
			File myFilePath = new File(logFile);
			try {
					if( !myFilePath.exists())
					{
							myFilePath.createNewFile();
							FileWriter resultFile = new FileWriter(myFilePath,true); 
							myFile = new PrintWriter(resultFile); 
					}
					FileWriter resultFile = new FileWriter(myFilePath,true); 
					myFile = new PrintWriter(resultFile); 
			} catch (IOException e) {
				e.printStackTrace();
			}
	 }
	 
	 public void setLunXunPath(String path)
	 {
		 queryPath=path;
	 }
	 
	 public void setFileSystem( FileSystem lfs)
	 {
		 fs = lfs;
	 }
	 
	public void setGenerateConfFile(String confFileName)
	{
		pdbssap.setGenerateConfFile(confFileName);
	}
	public void setCDRTableType( CDRTable.TYPE type)
	{
		pdbssap.setCDRTableType(type);
	}
	
	public void setFileList( 	ArrayList<Pair<String,Long>> lists )
	{
		list = lists;
		DebugPrint.DebugPrint("@gyy: fileList  length is " + lists.size());
		for( int i = 0 ; i < lists.size() ; i ++)
		{
			pdbssap.addCDRInputFileName( lists.get(i).getFirst(), lists.get(i).getSecond());
		}
	}
	
	synchronized public void AddLog(String name)
	{
		myFile.println("file is" + name);
		myFile.flush();
	}
	
	public void createIndex()
	{
		if(list.size() > 0)
		{
			pdbssap.doWithCDR();
			DebugPrint.DebugPrint("gyy:create index is over");
			for( int i=0;i<list.size();i++)
			{
				String lunPath = list.get(i).getFirst().replace('/', '#');
				lunPath = queryPath+"/" + lunPath.substring(1,lunPath.length());
				try {
					AddLog(lunPath);
					fs.delete(new Path(lunPath));
				} catch (IOException e) {
					e.printStackTrace();
				}
				DebugPrint.InforPrint("fileDelete : " + lunPath+" deleted");
			}				
		}
	}
}
