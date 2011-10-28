import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;


public class FileTest {
	

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		WriteThread  t = new WriteThread(SqlObject.PSMM);
		t.setF(new File("/media/30C8E0E2C8E0A776/ftp/PSMM/fsbsc1/psmm_hw20110719130000.txt"));
		new Thread(t).start();
		
		//FileInputStream in = new FileInputStream("");
		//BufferedReader br = new BufferedReader(new FileReader("/media/30C8E0E2C8E0A776/ftp/PSMM/fsbsc1/psmm_hw20110719130000.txt"));
		//br.readLine();
		//String str = br.readLine();
		//System.out.println(str.split(",").length);
		//System.out.println(str.split("\t")[295]);
		//System.out.println(str.split(",").length);
		
		/*File file = new File(PathName.PSMM);
		File[] files = file.listFiles();
		System.out.println(files.length);
		for(File f : files){
			//BufferedReader br = new BufferedReader(new FileReader(f));
			System.out.println(f.getAbsolutePath());
		}*/
		//for(int i =0; i<1000;i++){
		//	Connection conn = DBUtil.getConnection();
		//	System.out.println(conn);
		//}
		/*int i = 0;
		while(true){
			try{
				Connection conn = DBUtil.getConnection();
				System.out.println(conn);
			}catch (Exception e){
				e.printStackTrace();
				
				return;
			}
			i++;
			System.out.println(i);
		}*/
	

	}

}
