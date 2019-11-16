package com.uniplore;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.uniplore.tools.DataSourceManager;

public class BatchStart {

	private String vDataDate;
	public BatchStart(String vDataDate) {
		this.vDataDate = vDataDate;
	}
	
	public void start(){
		System.out.println("INFO"+"Today batch date is "+vDataDate);
		checkParam(vDataDate);
		updateBatchStartTime();
		initJobStatus();
		dealDayJob();
		dealMonthJob();
		dealQuartJob();
		dealYearJob();
		checkJob();
	}
	
	/**
	 * 检查参数，
	 * 如果有参数，插入跑批记录
	 * 如果没有参数，从数据库中获取
	 * @param vDataDate
	 */
	public void checkParam(String vDataDate){
		if(vDataDate!=null){
			this.vDataDate=vDataDate;
			Connection conn = null;
			try{			
				conn = DataSourceManager.getConnection();			
				PreparedStatement ps = 
						conn.prepareStatement("insert into ETL.EDW_BATCH_DATE(DATA_DATE)values (?)");
				ps.setString(1, vDataDate);
				ps.execute();
				ps.close();
			}catch(Exception e){
				e.printStackTrace();
			}finally{
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
				
			}
		}else{
			Connection conn = null;
			try{			
				conn = DataSourceManager.getConnection();			
				PreparedStatement ps = 
						conn.prepareStatement("SELECT MAX(DATA_DATE) FROM ETL.EDW_BATCH_DATE");
				ResultSet result=ps.executeQuery();
				result.next();
				vDataDate = result.getString(1);
				result.close();
				ps.close();
			}catch(Exception e){
				e.printStackTrace();
			}finally{
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
				
			}
		}
		
	}
	
	/**
	 * 更新跑批时间
	 * @return
	 */
	public boolean updateBatchStartTime(){
		Connection conn = null;
		boolean result = false;
		try{			
			conn = DataSourceManager.getConnection();			
			PreparedStatement ps = 
					conn.prepareStatement("UPDATE ETL.EDW_BATCH_DATE "
							+ "SET START_TIME=? "
							+ "WHERE DATA_DATE=?");
			SimpleDateFormat sdf =  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss" );
			ps.setString(1, sdf.format(new Date()));
			ps.setString(2, vDataDate);
			result=ps.execute();
			ps.close();
			System.out.println("INFO "+"Success to start batch.");
		}catch(Exception e){
			e.printStackTrace();
			System.out.println("ERROR"+"Can not update START_TIME.");
			System.out.println("DEBUG"+"DATA_DATE is ："+vDataDate);
		}finally{
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
		}
		return result;
	}
	
	/**
	 * 清理遗留跑批任务，为跑批做准备
	 * @return
	 */
	public boolean initJobStatus(){
		Connection conn = null;
		boolean result = false;
		try{	
			conn = DataSourceManager.getConnection();			
			PreparedStatement ps = 
					conn.prepareStatement("DELETE FROM ETL.EDW_JOB_STATUS "
							+ "WHERE DATA_DATE=?");
			ps.setString(1, vDataDate);
			result=ps.execute();
			ps.close();			
		}catch(Exception e){
			e.printStackTrace();
			System.out.println("ERROR"+"Fail to initinal job status.");
			System.out.println("DEBUG"+"DATA_DATE is ："+vDataDate);
		}finally{
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
		}
		return result;
	}
	
	/**
	 * 初始化处理周期为天的任务
	 * @return
	 */
	public boolean dealDayJob(){
		Connection conn = null;
		boolean result = false;
		try{	
			conn = DataSourceManager.getConnection();			
			PreparedStatement ps = 
					conn.prepareStatement("INSERT INTO ETL.EDW_JOB_STATUS"
							+ "(DATA_DATE,JOB_TYPE,JOB_FREQUENCY,JOB_DAY,JOB_GROUP,SEQ_NO,PROC_NAME) "
							+ "SELECT ?,JOB_TYPE,JOB_FREQUENCY,JOB_DAY,JOB_GROUP,SEQ_NO,PROC_NAME "
							+ "FROM ETL.EDW_JOB_CONTROL WHERE JOB_FREQUENCY='D' AND IF_VALID='Y'");
			ps.setString(1, vDataDate);
			result=ps.execute();
			ps.close();
			System.out.println("INFO "+"Success to initinal D job status.");
		}catch(Exception e){
			e.printStackTrace();
			System.out.println("ERROR "+"Fail to initinal D job status.");
			System.out.println("DEBUG "+"DATA_DATE is ："+vDataDate);
		}finally{
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
		}
		return result;
	}
	
	/**
	 * 初始化处理周期为月的任务
	 * @return
	 */
	public boolean dealMonthJob(){
		Connection conn = null;
		boolean result = false;
		try{
			String currentDay=vDataDate.substring(vDataDate.length()-2);
			conn = DataSourceManager.getConnection();			
			PreparedStatement ps = 
					conn.prepareStatement("INSERT INTO ETL.EDW_JOB_STATUS"
							+ "(DATA_DATE,JOB_TYPE,JOB_FREQUENCY,JOB_DAY,JOB_GROUP,SEQ_NO,PROC_NAME) "
							+ "SELECT ?,JOB_TYPE,JOB_FREQUENCY,JOB_DAY,JOB_GROUP,SEQ_NO,PROC_NAME "
							+ "FROM ETL.EDW_JOB_CONTROL "
							+ "WHERE JOB_FREQUENCY='M' AND JOB_DAY=? "
							+ "AND IF_VALID='Y'");
			ps.setString(1, vDataDate);
			ps.setString(2, currentDay);
			result=ps.execute();
			ps.close();
			System.out.println("INFO "+"Success to initinal M job status.");
		}catch(Exception e){
			e.printStackTrace();
			System.out.println("ERROR"+"Fail to initinal M job status.");
			System.out.println("DEBUG"+"DATA_DATE is ："+vDataDate);
		}finally{
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
		}
		return result;
	}
	
	/**
	 * 初始化处理周期为季度的任务
	 * @return
	 */
	public boolean dealQuartJob(){
		Connection conn = null;
		boolean result = false;
		try{
			String currentDay=vDataDate.substring(vDataDate.length()-2);
			conn = DataSourceManager.getConnection();			
			PreparedStatement ps = 
					conn.prepareStatement("INSERT INTO ETL.EDW_JOB_STATUS"
							+ "(DATA_DATE,JOB_TYPE,JOB_FREQUENCY,JOB_DAY,JOB_GROUP,SEQ_NO,PROC_NAME) "
							+ "SELECT ?,JOB_TYPE,JOB_FREQUENCY,JOB_DAY,JOB_GROUP,SEQ_NO,PROC_NAME "
							+ "FROM ETL.EDW_JOB_CONTROL "
							+ "WHERE JOB_FREQUENCY='Q' AND JOB_DAY=? "
							+ "AND IF_VALID='Y'");
			ps.setString(1, vDataDate);
			ps.setString(2, currentDay);
			result=ps.execute();
			ps.close();
			System.out.println("INFO "+"Success to initinal Q job status.");
		}catch(Exception e){
			e.printStackTrace();
			System.out.println("ERROR"+"Fail to initinal Q job status.");
			System.out.println("DEBUG"+"DATA_DATE is ："+vDataDate);
		}finally{
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
		}
		return result;
	}
	
	/**
	 * 初始化处理周期为年的任务
	 * @return
	 */
	public boolean dealYearJob(){
		Connection conn = null;
		boolean result = false;
		try{
			String currentDay=vDataDate.substring(vDataDate.length()-2);
			conn = DataSourceManager.getConnection();			
			PreparedStatement ps = 
					conn.prepareStatement("INSERT INTO ETL.EDW_JOB_STATUS"
							+ "(DATA_DATE,JOB_TYPE,JOB_FREQUENCY,JOB_DAY,JOB_GROUP,SEQ_NO,PROC_NAME) "
							+ "SELECT ?,JOB_TYPE,JOB_FREQUENCY,JOB_DAY,JOB_GROUP,SEQ_NO,PROC_NAME "
							+ "FROM ETL.EDW_JOB_CONTROL "
							+ "WHERE JOB_FREQUENCY='Y' AND JOB_DAY=? "
							+ "AND IF_VALID='Y'");
			ps.setString(1, vDataDate);
			ps.setString(2, currentDay);
			result=ps.execute();
			ps.close();
			System.out.println("INFO "+"Success to initinal Y job status.");
		}catch(Exception e){
			e.printStackTrace();
			System.out.println("ERROR"+"Fail to initinal M job status.");
			System.out.println("DEBUG"+"DATA_DATE is ："+vDataDate);
		}finally{
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
		}
		return result;
	}
	
	/**
	 * 根据任务初始化状态更新任务状态
	 * @return
	 */
	public boolean checkJob(){
		Connection conn = null;
		boolean result = false;
		try{
			conn = DataSourceManager.getConnection();			
			PreparedStatement ps = 
					conn.prepareStatement("SELECT COALESCE(COUNT(1),0) "
							+ "FROM ETL.EDW_JOB_STATUS "
							+ "WHERE DATA_DATE=?");
			ps.setString(1, vDataDate);
			ResultSet resultSet=ps.executeQuery();
			if(resultSet.next()){
				String jobNum = resultSet.getString(1);
				if(jobNum.equalsIgnoreCase("0")){
					ps=conn.prepareStatement("UPDATE ETL.EDW_BATCH_DATE "
							+ "SET STATUS='SKIP' "
							+ "WHERE DATA_DATE=?");
					ps.setString(1, vDataDate);
					ps.execute();
					ps.close();
					System.out.println("INFO "+"Success to update EDW_BATCH_DATE status.");
				}else{
					ps=conn.prepareStatement("UPDATE ETL.EDW_BATCH_DATE "
							+ "SET STATUS='RUN' "
							+ "WHERE DATA_DATE=?");
					ps.setString(1, vDataDate);
					ps.execute();
					ps.close();
					System.out.println("INFO "+"Success to update EDW_BATCH_DATE status.");
				}
			}
			result = true;
		}catch(Exception e){
			e.printStackTrace();
			System.out.println("ERROR"+"Fail to update job status.");
			System.out.println("DEBUG"+"DATA_DATE is ："+vDataDate);
		}finally{
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
		}
		return result;
	}

}
