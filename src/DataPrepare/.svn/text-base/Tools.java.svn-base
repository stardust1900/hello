package DataPrepare;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.SocketException;

import org.apache.commons.net.ftp.FTPClient;

public class Tools {
public static void main(String[] args) throws SocketException, IOException {
	FTPClient client = new FTPClient();
	client.connect("192.168.0.94",21);
	client.login("cstor", "cstor");
	client.changeWorkingDirectory("/DT");
	File file = new File("/temp/test.txt");
	if(!file.exists()){
		file.createNewFile();
	}
	OutputStream out = new BufferedOutputStream(new FileOutputStream(file));
	client.retrieveFile("/DT/DataExport_CDMA2_广州市黄浦萝岗区城郊数据上传_201108030947.txt", out);
}
}
