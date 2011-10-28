package createIndex;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import Data.CDRTable;
import Data.DateUtils;
import MyXMLReader.MyXmlReader;
import ParseData.ParseData;

public class generateIndex implements ParseData {
	public TreeMap<String, StringBuffer> treeMap = new TreeMap<String, StringBuffer>();
	public StringBuffer strBuf = new StringBuffer();
	// private final static int VERSION = 1;
	private static final String SPLIT = "\t";
	public String indexPath = "";// "/indexingCDR/iucs/tmp_index/";
	public String recordCounterPath = "";
	public String tmpName = "";// "tmp_x_index";
	public String mergedInfo = "";
	public String sourceRootPath = ""; // /CDRDIR/
	public static final int MERGER_FILE_NUM = 12;
	public long miniStartTime = Long.MAX_VALUE;
	public long maxStartTime = Long.MIN_VALUE;
	public Configuration conf;
	public DistributedFileSystem fs;
	public long totalCount = 0;
	public String conFileName = "";
	private String hdfsURL = "";
	public long cc1 = 0;
	public long cc2 = 0;
	public long recordNum = 0;
	private CDRTable.TYPE tableName = null;

	// CDR_1X, CDR_DO, DOSTREAM,PSMM,DT;

	public static final int[][] INDEX_TABLE = { { 0, 6, 9, 13, 60, 61 },
			{ 0, 1, 5, 6, 7, 12, 13 }, { 0, 1, 5, 6, 7, 12, 13 }, { 1, 7, 12 },
			{ 0, 1, 2, 3 } };

	private ArrayList<String> sourceFilePaths = new ArrayList<String>();

	public void addCDRInputFileName(String lName, long lfileNameID) {
		sourceFilePaths.add(lName);
	}

	public void setCDRTableType(CDRTable.TYPE ltype) {
		tableName = ltype;
	}

	public void setGenerateConfFile(String filename) {
		conFileName = filename;
	}

	public static void main(String[] args) {
		System.out.println("testing");
	}

	// CDR_1X, CDR_DO, DOSTREAM,PSMM,DT;
	public int decodeTableTypeByName(CDRTable.TYPE ltype) {
		if (ltype == CDRTable.TYPE.CDR_1X)
			return 0;
		else if (ltype == CDRTable.TYPE.CDR_DO)
			return 1;
		else if (ltype == CDRTable.TYPE.DOSTREAM)
			return 2;
		else if (ltype == CDRTable.TYPE.PSMM)
			return 3;
		return 4;
	}

	public String getHostName() {
		InetAddress ia = null;
		try {
			ia = InetAddress.getLocalHost();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (ia == null) {
			return "some error..";
		} else
			return ia.getHostName();
	}

	public void generateIndexFile(int tableType, ArrayList<String> pathes)
			throws IOException {
		String time = String.valueOf(new Date().getTime());
		MyXmlReader reader = new MyXmlReader(conFileName);
		hdfsURL = reader.getName("hdfsURL");
		sourceRootPath = reader.getName("sourceRoot");
		// TODO:CDR_1X, CDR_DO, DOSTREAM,PSMM,DT;
		recordCounterPath = reader.getName("recordCounterPath");// "/indexingCDR/CDRTotalNum/";
		System.out.println("sourceRootPath    is  "
				+ reader.getName("sourceRoot"));
		switch (tableType) {
		case 0:
			tmpName = "tmp_" + time + "_index";
			indexPath = reader.getName("cdr_1xIndexPath") + tmpName + "/";

			for (int i = 0; i < pathes.size(); i++)
				scanFileInfo(pathes.get(i), 7, 3, 0);
			break;
		case 1:
			tmpName = "tmp_" + time + "_index";
			indexPath = reader.getName("cdr_doIndexPath") + tmpName + "/";

			for (int i = 0; i < pathes.size(); i++)
				scanFileInfo(pathes.get(i), 2, 7, 1);
			break;
		case 2:
			tmpName = "tmp_" + time + "_index";
			indexPath = reader.getName("dostreamIndexPath") + tmpName + "/";
			for (int i = 0; i < pathes.size(); i++)
				scanFileInfo(pathes.get(i), 2, 7, 2);
			break;
		case 3:
			tmpName = "tmp_" + time + "_index";
			indexPath = reader.getName("psmmIndexPath") + tmpName + "/";
			for (int i = 0; i < pathes.size(); i++)
				scanFileInfo(pathes.get(i), 0, 1, 3);
			break;
		case 4:
			tmpName = "tmp_" + time + "_index";
			indexPath = reader.getName("dtIndexPath") + tmpName + "/";
			for (int i = 0; i < pathes.size(); i++)
				scanFileInfo(pathes.get(i), 5, 1, 4);
			break;
		default:
			break;
		}
	}

	public static boolean isLegalNum(String str) {
		boolean flag = true;
		try {
			Long.valueOf(str);
		} catch (Exception e) {
			flag = false;
			System.out.println(str);
		}
		return flag;
	}

	private void scanFileInfo(String path, int imsiIndex, int timeIndex,
			int type) throws IOException {
		int splitLen = 14;
		if (type == 0) {
			splitLen = 62;
		}
		FSDataInputStream in = fs.open(new Path(path));
		// FileInputStream fis = new FileInputStream(path);
		BufferedReader br = new BufferedReader(new InputStreamReader(in),
				65 * 1024 * 1024);
		String line = null;
		String yyyyMMdd = null;
		long offset = 0;
		if (type == 4) {// 建立时间
			if (path.endsWith("_1")) {
				line = br.readLine();
			}
			if (path.endsWith("_1") && line == null) {
				br.close();
				return;
			} else {
				offset = line.length() + 2;
				String temp = path.substring(path.lastIndexOf("/") + 1, path
						.lastIndexOf("."));
				String[] arr = temp.split("_");
				if (arr.length >= 1)
					yyyyMMdd = arr[0].trim();
			}
		}

		//
		StringBuffer sb = new StringBuffer();
		long tmpTime = -1;
		while ((line = br.readLine()) != null) {
			String[] arr = line.split(SPLIT, splitLen);
			if (arr.length != splitLen) {
				offset += line.length() + 2;
				continue;
			}

			String imsi = arr[imsiIndex].trim();
			String stime = arr[timeIndex].trim();
			if ("".equals(imsi)) {
				imsi = "0";
			}
			if ("".equals(stime)) {
				offset += line.length() + 2;
				continue;
			}

			if (type == 4) {
				try {
					tmpTime = DateUtils.getDate2Long(yyyyMMdd, stime);
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					offset += line.length() + 2;
					continue;
				}

			} else {
				try {
					tmpTime = DateUtils.getDate2Long(stime);
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					offset += line.length() + 2;
					continue;
				}
			}
			if (tmpTime < miniStartTime)
				miniStartTime = tmpTime;
			if (tmpTime > maxStartTime)
				maxStartTime = tmpTime;

			imsi = FormatePhoneNum(imsi);

			sb.append(path).append("%");
			for (int _i = 0; _i < INDEX_TABLE[type].length; _i++) {

				sb.append(arr[INDEX_TABLE[type][_i]]).append("%");

			}
			sb.append(offset).append("%").append(tmpTime);
			putInTreeTable(imsi, sb.toString());
			sb.delete(0, sb.length());
			offset += line.length() + 2;
		}
		br.close();

	}

	public boolean doWithCDR() {
		try {
			System.out.println("where there is a will ,there is a way");
			MyXmlReader reader = new MyXmlReader(conFileName);
			hdfsURL = reader.getName("hdfsURL");

			Date time1 = new Date();
			conf = new Configuration();
			fs = new DistributedFileSystem();
			fs.initialize(new URI(hdfsURL), conf);

			int tableType = decodeTableTypeByName(tableName);
			if (tableType == 4) {// dt的数据暂时不考虑
				return false;
			}
			// 按照相同一天的数据放在一起，保证不再一天的数据一定不再一起,,非dt的数据文件名是20110617090000.txt

			Map<String, ArrayList<String>> typeMap = new HashMap<String, ArrayList<String>>();
			for (int i = 0; i < sourceFilePaths.size(); i++) {
				String path = sourceFilePaths.get(i);
				// 取文件名中的日期
				String lastName = path.substring(path.lastIndexOf("/") + 1,
						path.lastIndexOf("."));
				String yyyyHHdd = lastName.substring(0, 8);
				if (typeMap.containsKey(yyyyHHdd)) {
					typeMap.get(yyyyHHdd).add(path);
				} else {
					ArrayList<String> list = new ArrayList<String>();
					list.add(path);
					typeMap.put(yyyyHHdd, list);
				}

			}

			for (String key : typeMap.keySet()) {
				ArrayList<String> arr = typeMap.get(key);

				if (arr.size() <= MERGER_FILE_NUM) {
					miniStartTime = Long.MAX_VALUE;
					maxStartTime = Long.MIN_VALUE;
					generateIndexFile(tableType, arr);
					writeToFile();
				} else {
					int j = 0;
					ArrayList<String> temp = new ArrayList<String>();
					for (int i = 0; i < arr.size(); i++) {
						temp.add(arr.get(i));
						j++;
						if (j % MERGER_FILE_NUM == 0) {
							miniStartTime = Long.MAX_VALUE;
							maxStartTime = Long.MIN_VALUE;
							generateIndexFile(tableType, temp);
							writeToFile();
							temp.clear();
						}

					}
					if (!temp.isEmpty()) {
						miniStartTime = Long.MAX_VALUE;
						maxStartTime = Long.MIN_VALUE;
						generateIndexFile(tableType, temp);
						writeToFile();
					}
				}
			}

			System.out.println("*****");

			// ShowHashTable();

			Date time2 = new Date();
			System.out.println("scan costs "
					+ (time2.getTime() - time1.getTime()) / 1000 + " seconds");
			System.out.println("scan costs "
					+ (time2.getTime() - time1.getTime()) + " ms");
			// System.out.println(cc1);
			// System.out.println(cc2);

			// for(int i=0;i<sourceFilePaths.size();i++)
			// {
			// String lunPath = sourceFilePaths.get(i).replace('/', '#');
			// lunPath = sourceRootPath+"/" +
			// lunPath.substring(1,lunPath.length());
			// fs.delete(new Path(lunPath));
			// System.out.println(lunPath+" deleted");
			// }
			Date time3 = new Date();
			System.out.println("write costs "
					+ (time3.getTime() - time2.getTime()) / 1000 + " seconds");
			System.out.println("write costs "
					+ (time3.getTime() - time2.getTime()) + " ms");

			System.out.println("Total costs "
					+ (time3.getTime() - time1.getTime()) / 1000 + " seconds");
			System.out.println("ToTAL costs "
					+ (time3.getTime() - time1.getTime()) + " ms");

			System.out.println("Scaned " + totalCount + " in total");
			System.out.println("MiniStartTime:" + miniStartTime);
			System.out.println("MaxStartTime:" + maxStartTime);

			// System.out.println(treeMap.size());
			// File f = new File("E:/testdata/CDR/index/");
			// String
			// strInfo="E:/testdata/CDR/index/".replaceFirst("index",String.valueOf(miniStartTime)+"_"+String.valueOf(maxEndTime));
			// System.out.println(strInfo);
			// File ft = new File(strInfo);
			// f.renameTo(ft);

			// String ss=readLineByOffset("C:/testdata/CDR/rawCDR1G.txt",
			// 10931429);
			// System.out.println(ss);
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	public void updateStartTime(String startTime) {
		long tmp = Long.valueOf(startTime);
		if (tmp < miniStartTime)
			miniStartTime = tmp;
		if (tmp > maxStartTime)
			maxStartTime = tmp;
	}

	public TreeMap<String, StringBuffer> getTreeMap() {
		return treeMap;
	}

	public void writeToFile() throws IOException {
		if (treeMap.isEmpty()) {
			return;
		}
		// 格式
		// 以文本形式，保存
		// 二级索引的总行数,二进制的int
		// 二级索引
		// 一级索引
		DataOutputStream dos = null;
		Path tempPath = new Path(indexPath + "merger.data");
		fs.mkdirs(new Path(indexPath));
		dos = new DataOutputStream(fs.create(tempPath));
		dos.writeInt(treeMap.size());
		dos.writeBytes("\r\n");
		long oneoffset = 2 + 4 + treeMap.size() * (16 + 1 + 16 + 2);//
		long ll = 0;
		for (Entry<String, StringBuffer> innerEntry : treeMap.entrySet()) {
			dos.writeBytes(innerEntry.getKey() + " "
					+ String.format("%016d", oneoffset) + "\r\n");
			oneoffset += innerEntry.getValue().toString().trim().length() + 2;

		}
		for (Entry<String, StringBuffer> innerEntry : treeMap.entrySet()) {
			dos.writeBytes(innerEntry.getValue().toString().trim() + "\r\n");
		}
		treeMap.clear();
		dos.close();

		String time = String.valueOf(new Date().getTime());

		fs.mkdirs(new Path(indexPath.replaceFirst(tmpName, miniStartTime + "_"
				+ maxStartTime + "_" + time)));
		fs.rename(tempPath, new Path(indexPath.replaceFirst(tmpName,
				miniStartTime + "_" + maxStartTime + "_" + time)));
		//	
		// fs.create(fs, new Path(recordCounterPath + recordNum + "_" + time),
		// new FsPermission((short) 777));
		fs.delete(new Path(indexPath), true);

		treeMap.clear();
	}

	public static String FormatePhoneNum(String phoneNum) {
		if (phoneNum.length() == 16)
			return phoneNum;
		else if (phoneNum.length() > 16)
			return phoneNum.substring(phoneNum.length() - 16);
		else
			return String.format("%016d", Long.valueOf(phoneNum));
	}

	public void ShowHashTable() {
		for (Entry<String, StringBuffer> entry : treeMap.entrySet()) {
			System.out.println(entry.getKey());
			System.out.println(entry.getValue());
			System.out.println("**********");
		}
	}

	public void putInTreeTable(String phoneNum, String info) {

		if (treeMap.containsKey(phoneNum)) {

			treeMap.put(phoneNum, treeMap.get(phoneNum).append(";")
					.append(info));
			// //String tmp=treeMap.get(phoneNum);
			// strBuf.append(treeMap.get(phoneNum));
			// strBuf.append(";");
			// strBuf.append(info);
			// //treeMap.put(phoneNum, tmp+";"+info);
			// treeMap.put(phoneNum, strBuf.toString());
			// strBuf.delete(0, strBuf.length());
		} else {

			treeMap.put(phoneNum, new StringBuffer(info));
		}
	}

	public void newFolder(String folderPath) {
		try {

			String filePath = folderPath;
			filePath = filePath.toString();
			File myFilePath = new File(filePath);
			if (!myFilePath.exists()) {
				myFilePath.mkdir();
			}
		} catch (Exception e) {
			System.out.println("");
			e.printStackTrace();
		}
	}
}
