package indexDelete;

import java.io.IOException;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

public class DeleteIndex {

	private static SimpleDateFormat dataFormat = new SimpleDateFormat(
			"yyyyMMdd");
	private DistributedFileSystem fs = new DistributedFileSystem();
	private static final Configuration conf = new Configuration();
	private String dir = null;
	private static final String[] TYPEDIR = { "/cdr_1x", "/cdr_do",
			"/dostream", "/psmm", "/dt" };

	public void init(String uri) throws IOException {
		fs.initialize(URI.create(uri), conf);
	}

	public void setIndexRootDir(String dir) {
		this.dir = dir;
	}

	public boolean deleteIndex(String dateStr) throws ParseException,
			IOException {
		Date date = dataFormat.parse(dateStr);
		long start = date.getTime();
		long end = this.getSpecifiedDayAfter(date);
		// System.out.println(start);
		// System.out.println(end);
		// TODO 遍历索引目录的文件夹
		// 五中类型都要处理
		for (int i = 0; i < TYPEDIR.length; i++) {
			this.delete(start, end, this.dir + TYPEDIR[i]);

		}

		return true;
	}

	private void delete(long start, long end, String path) throws IOException {
		FileStatus[] statusArr = fs.listStatus(new Path(path));
		for (int i = 0; i < statusArr.length; i++) {
			String dirName = statusArr[i].getPath().getName();
			if (!dirName.matches("^\\d+_\\d+_\\d+$"))
				continue;
			String[] temp = dirName.split("_", 2);
			long lstart = Long.parseLong(temp[0]);
			long lend = Long.parseLong(temp[1]);
			if (lstart >= start && end >= lend) {
				// 删除索引数据
				fs.delete(statusArr[i].getPath(), true);
			}

		}
		// /TelecomIndex/cdr_do

	}
	public void close() throws IOException
	{
		fs.close();
	}

	/**
	 * 获得指定日期的后一天
	 * 
	 * @param specifiedDay
	 * @return
	 */
	public long getSpecifiedDayAfter(Date date) {
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		int day = c.get(Calendar.DATE);
		c.set(Calendar.DATE, day + 1);
		return c.getTime().getTime();
	}

	/**
	 * @param args
	 * @throws ParseException
	 */
	public static void main(String[] args) throws ParseException {
		// TODO Auto-generated method stub

	}

}
