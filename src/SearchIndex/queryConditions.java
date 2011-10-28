package SearchIndex;
public class queryConditions
		{
			public long[] startTime;
			public long[] endTime;
			public long[] opc;
			public long[] dpc;
			public int tableType;
			public String cdrType;
			
			public int callType;
			public String phoneNum;
			public int wangYuanType;
			public String wangYuan;
			
			public long[] getOpc() {
				return opc;
			}

			public void setOpc(long[] opc) {
				this.opc = opc;
			}

			public long[] getDpc() {
				return dpc;
			}

			public void setDpc(long[] dpc) {
				this.dpc = dpc;
			}

			public queryConditions(long[] startTime,long[] endTime,int tableType,int callType,long[] opc,long[] dpc,String phoneNum)
			{
				this.startTime=startTime;
				this.endTime=endTime;
				this.tableType=tableType;
				this.callType=callType;
				this.opc=opc;
				this.dpc=dpc;
				this.phoneNum=phoneNum;
			}
			
			public queryConditions(long[] startTime,long[] endTime,int tableType,String cdrType,int callType,String phoneNum,int wangYuanType,String wangYuan)
			{
				this.setStartTime(startTime);
				this.setEndTime(endTime);
				this.setTableType(tableType);
				this.setCdrType(cdrType);
				this.setCallType(callType);
				this.setPhoneNum(phoneNum);
				this.setWangYuanType(wangYuanType);
				this.setWangYuan(wangYuan);
			}
			
			public queryConditions()
			{
				
			}
			
			public String  getPhoneNum()
			{
				return phoneNum;
			}
			public int getWangYuanType()
			{
				return wangYuanType;
			}
			
			public String getWangYuan()
			{
				return wangYuan;
			}
			
			public long[] getStartTime()
			{
				return startTime;
			}
			public long[] getEndTime()
			{
				return endTime;
			}
			public int getTableType(){
				return tableType;
			}
			public int getCallType()
			{
				return callType;
			}
			public String getCdrType()
			{
				return cdrType;
			}
			
			public void setPhoneNum(String  phoneNum)
			{
				this.phoneNum=String.valueOf(phoneNum);
			}
		
			public void setWangYuanType(int wangYuanType)
			{
				this.wangYuanType=wangYuanType;
			}
			
			public void setWangYuan(String wangYuan)
			{
				this.wangYuan=wangYuan;
			}
			
			public void setStartTime(long[] startTime)
			{
				this.startTime=startTime;
			}
			
			public void setEndTime(long[] endTime)
			{
				this.endTime=endTime;
			}
			
			public void setTableType(int tableType)
			{
				this.tableType=tableType;
			}
			public void setCallType(int callType)
			{
				this.callType=callType;
			}
			public void setCdrType(String cdrType)
			{
				this.cdrType=cdrType;
			}
		}
		