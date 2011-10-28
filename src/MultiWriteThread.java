import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;

public class MultiWriteThread implements Runnable{
	private File[] fArray;

	public void setF(File[] fArray) {
		this.fArray = fArray;
	}

	private String sql;
	private int fieldNum;
	private String tableName;

	public MultiWriteThread(String tableName) {
		this.tableName = tableName;
		if (SqlObject.CDR_1X.equals(tableName)) {
			sql = SqlObject.INSERT_CDR_1X;
			fieldNum = 612;
		} else if (SqlObject.DT.equals(tableName)) {
			sql = SqlObject.INSERT_DT;
			fieldNum = 296;
		} else if (SqlObject.PSMM.equals(tableName)) {
			sql = SqlObject.INSERT_CDR_PSMM;
			fieldNum = 20;
		}
	}

	@Override
	public void run() {
		Connection conn = null;
		BufferedReader br = null;
		try {
			long t1 = System.currentTimeMillis();
			conn = DBUtil.getConnection();
			Statement st = conn.createStatement();
			for (File f : fArray) {
				//System.out.println("file name : "+f.getName());
				br = new BufferedReader(new FileReader(f));
				if (SqlObject.DT.equals(tableName)) {
					br.readLine();
				}
				StringBuffer sb = new StringBuffer();
				sb.append(sql);
				int flag = 1;
				String[] s = null;
				while (br.read() > -1) {
					String tmp = br.readLine();
					if (SqlObject.DT.equals(tableName)) {
						s = tmp.split("\t");
					} else if (tmp.endsWith(",")) {
						s = (tmp + "0x").split(",");
					} else {
						s = tmp.split(",");
					}
					if(s.length < fieldNum){
						continue;
					}
					sb.append("(");
					for (int i = 0; i < fieldNum; i++) {
						if (s[i] != null) {
							if(s[i].length()>50){
								sb.append("'").append(s[i].substring(0, 50)).append("',");
							}else{
								sb.append("'").append(s[i]).append("',");
							}
						} else {
							sb.append("null,");
						}
						
					}
					sb.deleteCharAt(sb.length() - 1);
					if (flag % 100 == 0) {
						sb.append(")");
						// System.out.println(sb.toString());
						st.execute(sb.toString());
						sb.delete(0, sb.length());
						sb.append(sql);
					} else {
						sb.append("),");
					}
					flag++;

				}
				if ((flag - 1) % 100 != 0) {
					sb.deleteCharAt(sb.length() - 1);
					// System.out.println(sb.toString());
					st.execute(sb.toString());
				}
				SimpleDateFormat df = new SimpleDateFormat(
						"yyyy-MM-dd hh:mm:ss");
				System.out.println(f.getName() + " end!" + "  time is :"+ df.format(new Date()) +  " | "+(flag - 1) + " rows used: " + (System.currentTimeMillis() - t1));
				System.out.println();
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

	}
}
