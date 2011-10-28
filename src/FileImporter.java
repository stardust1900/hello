import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import org.apache.log4j.Logger;


public class FileImporter {
	private static Logger logger = Logger.getLogger(FileImporter.class);  
	
	public static void main(String[] args){
		//getFiles(PathName.CDRFS,SqlObject.CDR_1X);
		//getFiles(PathName.CDRGZ,SqlObject.CDR_1X);
		//getFiles(PathName.DT,SqlObject.DT);
		//getFiles(PathName.PSMM,SqlObject.PSMM);
		
		/*startMultiThread(PathName.CDRFS,SqlObject.CDR_1X);
		startMultiThread(PathName.CDRGZ,SqlObject.CDR_1X);
		startMultiThread(PathName.DT,SqlObject.DT);
		startMultiThread(PathName.PSMM,SqlObject.PSMM);*/
		logger.debug("debug---------");
		
		logger.info("info-----------");
		
		logger.error("error----------");
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		System.out.println("  time is :" +df.format(new Date()));
	}
	public static void startMultiThread(String path, String tableName){
		File file = new File(path);
		File[] files = file.listFiles();
		int num = files.length/10;
		System.out.println("num : "+ num);
		ArrayList<File[]> flist = new ArrayList<File[]>();
		for(int i=0;i<num;i++){
			flist.add(new File[10]);
		}
		flist.add(new File[files.length%10]);
		//System.arraycopy(src, srcPos, files, destPos, length)
		int i = 0;
		for(File[] fArray : flist){
			System.arraycopy(files, i, fArray, 0, fArray.length);
			//System.out.println(fArray[0]);
			MultiWriteThread t = new MultiWriteThread(tableName);
			t.setF(fArray);
			new Thread(t).start();
			i = i+10;
		}
		
	}
	
	public static void getFiles(String path,String tableName){
		File file = new File(path);
		File[] files = file.listFiles();
		System.out.println(files.length);
		for(File f : files){
			WriteThread t = new WriteThread(tableName);
			t.setF(f);
			new Thread(t).start();
			System.out.println("thread start : "+ f.getName());
		}
	}
}
