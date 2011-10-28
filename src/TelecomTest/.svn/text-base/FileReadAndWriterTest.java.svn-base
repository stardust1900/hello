package TelecomTest;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class FileReadAndWriterTest {

	private File file = new File("e:/temp_test_cdr/test");
	public void read(){
		try {
			BufferedReader reader = new BufferedReader(new FileReader(file));
			String str = null;
			while((str=reader.readLine())!=null){
				System.out.println(str);
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	public void writer(){
		try {
			BufferedWriter writer = new BufferedWriter(new FileWriter(file));
			for(int i=0;i<1000;i++){
				writer.write("wyr"+i+"\r\n");
				writer.flush();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public static void main(String[]args){
	      FileReadAndWriterTest fileReadAndWriterTest = new FileReadAndWriterTest();
	      fileReadAndWriterTest.writer();
	      fileReadAndWriterTest.read();
	}
}
