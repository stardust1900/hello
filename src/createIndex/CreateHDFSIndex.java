package createIndex;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.concurrent.BlockingQueue;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Pair;

import Data.CDRTable;
import MyXMLReader.MyXmlReader;
import Tools.DebugPrint;

public class CreateHDFSIndex implements Runnable {

	private boolean isStop = false;
	private BlockingQueue<Pair<String, String>> queue = null;
	String fileName = null;
	CDRTable.TYPE type = null;

	String cut = "@";

	ReadFromHDFSClient parent = null;
	String content = "";
	String nodeName = "";
	String confFileName = "";
	String hdfsURL = "";
	String queryPath = "";
	FileSystem fs = null;
	String fileLog = "";

	public CreateHDFSIndex(BlockingQueue<Pair<String, String>> queue,
			ReadFromHDFSClient lparent, String lConfFileName, FileSystem lfs) {
		this.queue = queue;
		parent = lparent;
		confFileName = lConfFileName;
		fs = lfs;
	}

	public void InitConf() {
		MyXmlReader reader = new MyXmlReader(confFileName);
		queryPath = reader.getName("sourceRoot");
		System.out.println("queryPath  is " + queryPath);
		fileLog = reader.getName("fileLog");

	}

	// for hdfs index
	public void run() {
		InitConf();

		while (!isStop) {
			Pair<String, String> reslut = null;

			try {
				reslut = queue.take();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			DebugPrint.DebugPrint("threadRequest is " + reslut.getFirst());
			content = reslut.getFirst();
			nodeName = reslut.getSecond();
			RunHDFS();
		}

	}

	public void RunHDFS() {
		ArrayList<Pair<String, Long>> CDR_1X = new ArrayList<Pair<String, Long>>();
		ArrayList<Pair<String, Long>> CDR_DO = new ArrayList<Pair<String, Long>>();
		ArrayList<Pair<String, Long>> DOSTREAM = new ArrayList<Pair<String, Long>>();
		ArrayList<Pair<String, Long>> PSMM = new ArrayList<Pair<String, Long>>();
		ArrayList<Pair<String, Long>> DT = new ArrayList<Pair<String, Long>>();
		ArrayList<Pair<String, Long>> events = new ArrayList<Pair<String, Long>>();

		StringTokenizer as = new StringTokenizer(content, cut);
		while (as.hasMoreTokens()) {
			String fileName = as.nextToken();
			fileName = "/" + fileName.replace("#", "/");
			System.out.println("fileName is " + fileName);
			long fileNumber = Long.parseLong(as.nextToken());

			// �����bssap
			if (fileName.matches(".+cdr1x.*")) {// CDR_1X
				if (!fileName.matches(".+cdr_1xEvent.*")) {
					CDR_1X.add(new Pair(fileName, fileNumber));
				} else {
					events.add(new Pair(fileName, fileNumber));
				}
			} else if (fileName.matches(".+cdrdo.*")) {// CDR_DO

				if (!fileName.matches(".+cdr_doEvent.*")) {
					CDR_DO.add(new Pair(fileName, fileNumber));
				} else {
					events.add(new Pair(fileName, fileNumber));
				}
			} else if (fileName.matches(".+dostream.*")) {// DOSTREAM

				if (!fileName.matches(".+dostreamEvent.*")) {
					DOSTREAM.add(new Pair(fileName, fileNumber));
				} else {
					events.add(new Pair(fileName, fileNumber));
				}
			} else if (fileName.matches(".+psmm.*")) {// PSMM

				if (!fileName.matches(".+psmmEvent.*")) {
					PSMM.add(new Pair(fileName, fileNumber));
				} else {
					events.add(new Pair(fileName, fileNumber));
				}
			} else if (fileName.matches(".+dt.*")) {// DT

				if (!fileName.matches(".+dtEvent.*")) {
					DT.add(new Pair(fileName, fileNumber));
				} else {
					events.add(new Pair(fileName, fileNumber));
				}
			}
		}
		// CDR_1X, ,
		if (CDR_1X.size() > 0) {
			CreateHDFSIndexImp _CDR_1X = new CreateHDFSIndexImp();
			_CDR_1X.setGenerateConfFile(confFileName);
			_CDR_1X.setCDRTableType(CDRTable.TYPE.CDR_1X);
			_CDR_1X.setLunXunPath(queryPath);
			_CDR_1X.setFileList(CDR_1X);
			_CDR_1X.setLogFile(fileLog);
			_CDR_1X.setFileSystem(fs);
			_CDR_1X.createIndex();
			_CDR_1X = null;
		}
		// CDR_DO
		if (CDR_DO.size() > 0) {
			CreateHDFSIndexImp _CDR_DO = new CreateHDFSIndexImp();
			_CDR_DO.setGenerateConfFile(confFileName);
			_CDR_DO.setCDRTableType(CDRTable.TYPE.CDR_DO);
			_CDR_DO.setLunXunPath(queryPath);
			_CDR_DO.setFileList(CDR_DO);
			_CDR_DO.setLogFile(fileLog);
			_CDR_DO.setFileSystem(fs);
			_CDR_DO.createIndex();
			_CDR_DO = null;
		}
		// DOSTREAM,
		if (DOSTREAM.size() > 0) {
			CreateHDFSIndexImp _DOSTREAM = new CreateHDFSIndexImp();
			_DOSTREAM.setGenerateConfFile(confFileName);
			_DOSTREAM.setLunXunPath(queryPath);
			_DOSTREAM.setCDRTableType(CDRTable.TYPE.DOSTREAM);
			_DOSTREAM.setFileList(DOSTREAM);
			_DOSTREAM.setLogFile(fileLog);
			_DOSTREAM.setFileSystem(fs);
			_DOSTREAM.createIndex();
			_DOSTREAM = null;
		}
		// PSMM;
		if (PSMM.size() > 0) {
			CreateHDFSIndexImp _PSMM = new CreateHDFSIndexImp();
			_PSMM.setGenerateConfFile(confFileName);
			_PSMM.setLunXunPath(queryPath);
			_PSMM.setCDRTableType(CDRTable.TYPE.PSMM);
			_PSMM.setFileList(PSMM);
			_PSMM.setLogFile(fileLog);
			_PSMM.setFileSystem(fs);
			_PSMM.createIndex();
			_PSMM = null;
		}
		// DT
		if (DT.size() > 0) {
			CreateHDFSIndexImp _DT = new CreateHDFSIndexImp();
			_DT.setGenerateConfFile(confFileName);
			_DT.setLunXunPath(queryPath);
			_DT.setCDRTableType(CDRTable.TYPE.DT);
			_DT.setFileList(DT);
			_DT.setLogFile(fileLog);
			_DT.setFileSystem(fs);
			_DT.createIndex();
			_DT = null;
		}
		int i = 0;
		for (i = 0; i < events.size(); i++) {
			String lunPath = events.get(i).getFirst().replace('/', '#');
			lunPath = queryPath + "/" + lunPath.substring(1, lunPath.length());
			System.out.println("path  is " + lunPath);
			try {
				fs.delete(new Path(lunPath));
			} catch (IOException e) {
				e.printStackTrace();
			}
			System.out.println(lunPath + " deleted");
		}
		parent.RequestIsOver(nodeName);
		// CDR_1X, CDR_DO, DOSTREAM,PSMM,DT;
		CDR_1X.clear();
		CDR_DO.clear();
		DOSTREAM.clear();
		PSMM.clear();
		DT.clear();
		events.clear();

		CDR_1X = null;
		CDR_DO = null;
		DOSTREAM = null;
		PSMM = null;
		DT = null;
		events = null;
	}
}
