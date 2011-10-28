package DataPrepare;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import MyXMLReader.MyXmlReader;

public class WriteDataToHdfs {

	private String hostUrl;
	private String cdrDataDir;
	private String dtDataDir;
	private int cdrMergeRowsCount;
	private int dtMergeRowsCount;
	private int cdrRowsCount;
	private int dtRowsCount;
	private String currentCdrFileName;
	private String currentDtFileName;
	private Configuration conf;
	private FileSystem fs;
	private Path cdrPath;
	private Path dtPath;

	public WriteDataToHdfs(String path) throws Exception {
		MyXmlReader reader = new MyXmlReader(path);
		hostUrl = reader.getName("hosturl");
		cdrDataDir = reader.getName("cdrdatadir");
		dtDataDir = reader.getName("dtdatadir");
		cdrMergeRowsCount = Integer.parseInt(reader.getName("cdrmergerowscount"));
		dtMergeRowsCount = Integer.parseInt(reader.getName("dtmergerowscount"));
		dtRowsCount = 0;
	    conf = new Configuration();
	    fs = FileSystem.get(URI.create(hostUrl),conf);
	}

	public void writeCdrFilesForPrepare(String str,String cdrFileUri) throws Exception {
		FSDataOutputStream writer = null;
		if(fs.exists(new Path(cdrFileUri))){
			writer = fs.append(new Path(cdrFileUri));
		}else{
			writer = fs.create(new Path(cdrFileUri));
		}
	    writer.write((str+"\r\n").getBytes());
	    writer.flush();
	}

	public void writeDtFilesForPrepare(BufferedReader reader) throws Exception {
		FSDataOutputStream writer = null ;
		currentDtFileName = dtDataDir + "dt_"+ System.currentTimeMillis();
		dtPath = new Path(currentDtFileName);
		writer = fs.create(dtPath);
		String str =null;	
		while((str = reader.readLine())!=null){
			if (dtRowsCount == dtMergeRowsCount) {
				currentDtFileName = dtDataDir + "dt_"+System.currentTimeMillis();
				dtPath = new Path(currentDtFileName);
				writer = fs.create(dtPath);
				dtRowsCount = 0;
			}
			writer.write((str+"\r\n").getBytes());
			writer.flush();
			dtRowsCount++;
		}
		reader.close();
		writer.close();
	}

	public static void main(String[] args) throws Exception {
		BufferedReader cdrReader = new BufferedReader(new FileReader(new File("/home/wyr/cdr_test")));
		BufferedReader dtReader = new BufferedReader(new FileReader(new File("/home/wyr/dt_test")));
		WriteDataToHdfs writeDataToHdfs = new WriteDataToHdfs("/home/wyr/conf/nodeInit_wyr.xml");
		writeDataToHdfs.writeDtFilesForPrepare(dtReader);
	}
}
