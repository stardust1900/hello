package Data;

import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.concurrent.BlockingQueue;

import org.apache.hadoop.fs.FSDataInputStream;


public interface WriteObject {
	
	public void 	  AddWriteList(BlockingQueue queue);
	public void 	  Write(FileOutputStream out);
	public boolean ReadCDRFromHDFS(FSDataInputStream stream);
	public boolean Read( InputStream finput);
	public boolean decode(byte[] content) ;


}
