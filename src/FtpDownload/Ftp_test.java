package FtpDownload;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.*;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;


public class Ftp_test {
	
	private FTPClient ftpClient;
	
	public Ftp_test(String host, String user, String password, int port) {
		ftpClient = new FTPClient();
		try {
			ftpClient.connect(host, port);
			ftpClient.login(user, password);
		} catch (IllegalStateException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 验证是否登录成功
	 * 
	 * @return
	 */
	
	public boolean isConnected() {
		return ftpClient.isConnected();
	}
	
	/**
	 * 删除文件
	 * @return
	 */
	public void deleFile(String pathname) throws IOException{
		try {
			if(isConnected()){
				ftpClient.dele(pathname);
			} }catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		}

	/**
	 * 下载文件
	 * @param resFilePath 下载的文件原来所在的路径
	 * @param targetFilePath 下载的文件准备存放的目的路径
	 */
    public boolean download(String remoteFileName, String localFileName)    
            throws IOException {
    	
        boolean flag = false;
        File outfile = new File(localFileName);    
        OutputStream oStream = null;    
        try {
            oStream = new FileOutputStream(outfile);
            flag = ftpClient.retrieveFile(remoteFileName, oStream);
        } catch (IOException e) {
            flag = false;
            return flag;
        } finally {
            oStream.close(); 
        }    
        return flag; 
    }
	
    
    /**  
     * Description: 从FTP服务器下载文件  
     * @param url FTP服务器hostname  
     * @param port FTP服务器端口  
     * @param username FTP登录账号  
     * @param password FTP登录密码  
     * @param remotePath FTP服务器上的相对路径  
     * @param fileName 要下载的文件名  
     * @param localPath 下载后保存到本地的路径  
     * @return  
     */   
//    public static boolean downFile(String url, int port,String username, String password, String remotePath,String fileName,String localPath) {   
//        boolean success = false;   
//        FTPClient ftp = new FTPClient();   
//        try {   
//            int reply;   
//            ftp.connect(url, port);   
//            //如果采用默认端口，可以使用ftp.connect(url)的方式直接连接FTP服务器   
//            ftp.login(username, password);//登录   
//            reply = ftp.getReplyCode();   
//            if (!FTPReply.isPositiveCompletion(reply)) {   
//                ftp.disconnect();   
//                return success;
//            }   
//            ftp.changeWorkingDirectory(remotePath);//转移到FTP服务器目录   
//            FTPFile[] fs = ftp.listFiles();   
//            for(FTPFile ff:fs){   
//                if(ff.getName().equals(fileName)){   
//                    File localFile = new File(localPath+"/"+ff.getName());   
//                       
//                    OutputStream is = new FileOutputStream(localFile);    
//                    ftp.retrieveFile(ff.getName(), is);   
//                    is.close();   
//                }   
//            }   
//               
//            ftp.logout();   
//            success = true;   
//        } catch (IOException e) {   
//            e.printStackTrace();   
//        } finally {   
//            if (ftp.isConnected()) {   
//                try {   
//                    ftp.disconnect();   
//                } catch (IOException ioe) {   
//                }   
//            }   
//        }   
//        return success;   
//    }

    
    public boolean download2(String remoteFileName)    
            throws IOException {
//        boolean flag = false;    
//        File outfile = new File(localFileName);
//        OutputStream oStream = null;
        boolean flag = true;
		try {
			FTPFile[] fs = ftpClient.listFiles();   
	        for(FTPFile ff:fs){
	            if(ff.getName().equals(-1)){
	            	System.out.println(ff.getName());
	            }
	        }
//            oStream = new FileOutputStream(outfile);
//			BufferedInputStream reader1=new BufferedInputStream(new FileInputStream(remoteFileName));
//            BufferedReader reader = new BufferedReader(new FileReader(remoteFileName));
//            String tempString=null;
            // 一次读入一行，直到读入null为文件结束
//            while (( reader.readLine()) != null) {
//                // 显示行号
//                System.out.println(tempString);
//            }
//           reader.close();
        } catch (IOException e) {
            flag = false;
            return flag;
        } finally {
//            oStream.close();
        }    
        return flag;
    }
    
    
	public void disconnect(){
		try {
			ftpClient.disconnect();
		} catch (IllegalStateException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void test(String fileName) throws IOException{
		
		ftpClient.changeWorkingDirectory(fileName);
		FTPFile[] ftpFiles = ftpClient.listFiles();
//		InputStream input = ftpClient.retrieveFileStream(fileName);
		for(FTPFile ftpFile : ftpFiles){
			System.out.println("xxx"+ftpFile.getName());
		}
//		BufferedReader reader = new BufferedReader(new InputStreamReader(input));
//		System.out.println(ftpFiles.length);
	}
	
	/**
	 *  Ftp_test类运行测试
	 * @return
	 * @throws IOException 
	 */
	public static void main (String[] args) throws IOException{
//		FTPFile[] wo=ftp.getFileList("/");
//		System.out.println(wo);
		
		
//		try {
//			Ftp_test ftp= new Ftp_test("192.168.0.213","cstor","cstor",21);
//			ftp.download2("txt");
//			System.out.println("download is finished");
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//			System.out.println("no file");
//		}
		
		Ftp_test ftp = new Ftp_test("192.168.11.54", "cstor", "cstor", 21);
		ftp.test("wo.txt");
	}
	
}
