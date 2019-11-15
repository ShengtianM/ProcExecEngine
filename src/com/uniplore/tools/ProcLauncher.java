package com.uniplore.tools;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ProcLauncher {

	private String vDataDate;
	private String jobType;
	private int index;
	private int num;
	
	/**
	 * 
	 * @param vDataDate 时间
	 * @param jobType 任务类型
	 * @param index 并行序号
	 * @param num 并行数量
	 */
	public ProcLauncher(String vDataDate,String jobType,int index,int num) {
		this.vDataDate = vDataDate;
		this.jobType = jobType;
		this.index = index;
		this.num = num;
	}
	
	public void startWork(){
		checkParam();
		work(vDataDate, jobType);
	}
	
	public boolean checkParam(){
		return true;
	}
	
	/**
	 * 初始化任务开始时间
	 * @param vDataDate
	 * @param jobType
	 * @param jobGroup
	 * @param procName
	 */
	public void start(String vDataDate,String jobType,String jobGroup,String procName){
		Connection conn = null;
		try{	
			conn = DataSourceManager.getConnection();			
			PreparedStatement ps = 
					conn.prepareStatement("UPDATE ETL.EDW_JOB_STATUS "
							+ "SET START_TIME=to_char(NOW(),'yyyy-mm-dd hh:mm:ss') "
							+ "WHERE DATA_DATE=? AND JOB_TYPE=? "
							+ "AND JOB_GROUP=? AND PROC_NAME=?");
			ps.setString(1, vDataDate);
			ps.setString(2, jobType);
			ps.setString(3, jobGroup);
			ps.setString(4, procName);
			ps.execute();
			ps.close();
			System.out.println("INFO "+"Success to update "+procName+" the status.");
		}catch(Exception e){
			e.printStackTrace();
			System.out.println("ERROR"+"Fail to update the status.");
			System.out.println("DEBUG"+"DATA_DATE is ："+vDataDate);
		}finally{
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
		}
	}
	
	/**
	 * 更新任务结束时间
	 * @param vDataDate
	 * @param jobType
	 * @param jobGroup
	 * @param procName
	 */
	public void end(String vDataDate,String jobType,String jobGroup,String procName){
		Connection conn = null;
		try{	
			conn = DataSourceManager.getConnection();			
			PreparedStatement ps = 
					conn.prepareStatement("UPDATE ETL.EDW_JOB_STATUS "
							+ "SET END_TIME=to_char(NOW(),'yyyy-mm-dd hh:mm:ss') "
							+ "WHERE DATA_DATE=? AND JOB_TYPE=? "
							+ "AND JOB_GROUP=? AND PROC_NAME=?");
			ps.setString(1, vDataDate);
			ps.setString(2, jobType);
			ps.setString(3, jobGroup);
			ps.setString(4, procName);
			ps.execute();
			
			ps = 
					conn.prepareStatement("UPDATE ETL.EDW_JOB_STATUS "
							+ "SET RUN_TIME=NULL "
							+ "WHERE DATA_DATE=? AND JOB_TYPE=? "
							+ "AND JOB_GROUP=? AND PROC_NAME=?");
			ps.setString(1, vDataDate);
			ps.setString(2, jobType);
			ps.setString(3, jobGroup);
			ps.setString(4, procName);
			ps.execute();
			ps.close();
		}catch(Exception e){
			e.printStackTrace();
			System.out.println("ERROR"+"Fail to update the status.");
			System.out.println("DEBUG"+"DATA_DATE is ："+vDataDate);
		}finally{
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
		}
	}
	
	/**
	 * 修改任务状态
	 * @param status 需要修改的状态
	 * @param vDataDate
	 * @param jobType
	 * @param jobGroup
	 * @param procName
	 */
	public void changeStatus(String status,String vDataDate,String jobType,String jobGroup,String procName){
		Connection conn = null;
		try{	
			conn = DataSourceManager.getConnection();			
			PreparedStatement ps = 
					conn.prepareStatement("UPDATE ETL.EDW_JOB_STATUS "
							+ "SET STATUS=? "
							+ "WHERE DATA_DATE=? AND JOB_TYPE=? "
							+ "AND JOB_GROUP=? AND PROC_NAME=?");
			ps.setString(1, status);
			ps.setString(2, vDataDate);
			ps.setString(3, jobType);
			ps.setString(4, jobGroup);
			ps.setString(5, procName);
			ps.execute();
			ps.close();
		}catch(Exception e){
			e.printStackTrace();
			System.out.println("ERROR"+" Fail to update the status.");
			System.out.println("DEBUG"+" DATA_DATE is ："+vDataDate);
		}finally{
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
		}
	}
	
	/**
	 * 主要工作函数，按任务组顺序执行
	 * @param vDataDate
	 * @param jobType
	 */
	public void work(String vDataDate,String jobType){
		Connection conn = null;
		try{	
			conn = DataSourceManager.getConnection();			
			PreparedStatement ps = 
					conn.prepareStatement("SELECT DISTINCT JOB_GROUP "
							+ "FROM ETL.EDW_JOB_STATUS "
							+ "WHERE DATA_DATE=? "
							+ "AND JOB_TYPE=? ORDER BY JOB_GROUP");
			ps.setString(1, vDataDate);
			ps.setString(2, jobType);
			ResultSet rs = ps.executeQuery();
			List<String> groupList = new ArrayList<String>();
			while(rs.next()){
				groupList.add(rs.getString(1));
			}
			rs.close();
			for(String groupName:groupList){
				System.out.println("INFO"+"[----------Start to group:"
						+ groupName+"-------------]");
				List<String> procList = startGroup(groupName);
				
				for(String proc:procList){
					System.out.println("INFO"+"[----------Start to proc:"
							+ proc+"-------------]");
					start(vDataDate, jobType, groupName, proc);
					switch(jobType){
					case "HIVE":
						break;
					case "GP":
						changeStatus("RUN", vDataDate, jobType, groupName, proc);
						ps = conn.prepareStatement("SELECT "+ proc+"(?)");
						ps.setString(1, vDataDate);
						try{
							ps.execute();
							changeStatus("DONE", vDataDate, jobType, groupName, proc);							
						}catch(Exception e){
							changeStatus("FAIL", vDataDate, jobType, groupName, proc);
							e.printStackTrace();
							System.out.println("ERROR"+"Fail to run procedure :"+proc);
							System.out.println("DEBUG"+"DATA_DATE is ："+vDataDate);
						}
						end(vDataDate, jobType, groupName, proc);
						break;
					}
				}
				
				while(true){
					PreparedStatement ps2 = 
							conn.prepareStatement("SELECT COALESCE(COUNT(1),0) "
									+ "FROM ETL.EDW_JOB_STATUS "
									+ "WHERE DATA_DATE=? "
									+ "AND JOB_TYPE=? "
									+ "AND JOB_GROUP=? "
									+ "AND (STATUS IS NULL OR STATUS='RUN')");
					ps2.setString(1, vDataDate);
					ps2.setString(2, jobType);
					ps2.setString(3, groupName);
					ResultSet rs2 = ps2.executeQuery();
					rs2.next();
					int count = Integer.parseInt(rs2.getString(1));
					rs2.close();
					ps2.close();
					if(count==0){
						break;
					}else{
						Thread.sleep(10000);
					}
					
				}
				
				if(!checkGroupStatus(vDataDate, jobType, groupName)){
					System.out.println("ERROR "+"Group："+
							groupName+" has fail procedures, pls Check table EDW_JOB_STATUS.");
					break;
				}else{
					System.out.println("INFO "+"Group："+groupName+" has done.");
				}
			}
			ps.close();
		}catch(Exception e){
			e.printStackTrace();
			System.out.println("ERROR"+"Fail to update the status.");
			System.out.println("DEBUG"+"DATA_DATE is ："+vDataDate);
		}finally{
			try {
				if(conn!=null){
					conn.close();
				}
				
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
		}
	}
	
	/**
	 * 根据组名获取任务列表
	 * @param groupName
	 * @return
	 */
	public List<String> startGroup(String groupName){
		Connection conn = null;
		List<String> procList = new ArrayList<String>();
		try{
			conn = DataSourceManager.getConnection();
		PreparedStatement ps1 = 
				conn.prepareStatement("SELECT DISTINCT PROC_NAME "
						+ "FROM ETL.EDW_JOB_STATUS "
						+ "WHERE DATA_DATE=? AND JOB_TYPE=? "
						+ "AND JOB_GROUP=? AND "
						+ "MOD(SEQ_NO,?)=? "
						+ "AND STATUS IS NULL GROUP BY PROC_NAME");
		ps1.setString(1, vDataDate);
		ps1.setString(2, jobType);
		ps1.setString(3, groupName);
		ps1.setInt(4, num);
		ps1.setInt(5, index);		
		ResultSet rs1 = ps1.executeQuery();		
		while(rs1.next()){
			procList.add(rs1.getString(1));
		}
		rs1.close();
		ps1.close();
		}catch(Exception e){
			e.printStackTrace();
			System.out.println("ERROR "+"Fail to update the status.");
			System.out.println("DEBUG "+"DATA_DATE is ："+vDataDate);
		}finally{
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
		}
		
		return procList;
	}
	
	/**
	 * 检查组状态
	 * @param vDataDate
	 * @param jobType
	 * @param jobGroup
	 * @return
	 */
	public boolean checkGroupStatus(String vDataDate,String jobType,String jobGroup){
		Connection conn = null;
		boolean doneFlag = false;
		try{	
			conn = DataSourceManager.getConnection();			
			PreparedStatement ps = 
					conn.prepareStatement("SELECT COALESCE(COUNT(1),0) "
							+ "FROM ETL.EDW_JOB_STATUS "
							+ "WHERE DATA_DATE=? AND JOB_TYPE=? "
							+ "AND JOB_GROUP=? AND STATUS='FAIL'");
			ps.setString(1, vDataDate);
			ps.setString(2, jobType);
			ps.setString(3, jobGroup);
			ResultSet rs = ps.executeQuery();
			rs.next();
			int count = Integer.parseInt(rs.getString(1));
			if(count==0){
				doneFlag = true;
			}
			rs.close();
			ps.close();
		}catch(Exception e){
			e.printStackTrace();
			System.out.println("ERROR"+"Fail to update the status.");
			System.out.println("DEBUG"+"DATA_DATE is ："+vDataDate);
		}finally{
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
		}
		return doneFlag;
	}
	

}
