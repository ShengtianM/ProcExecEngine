package com.uniplore.tools;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ForkJoinPool;

import com.uniplore.JobTask;

public class ProcParallelLauncher {

	private String jobType;
	private String vDataDate;
	private int parallelNum;
	/**
	 * 根据JOB类型并行执行任务
	 * @param jobType HIVE GP
	 */
	public ProcParallelLauncher(String date,String jobType,int parallelNum) {
		this.vDataDate = date;
		this.jobType = jobType;
		this.parallelNum = parallelNum;
	}
	
	public void start(){
		checkParam();
		initJobStatus();
		boolean run=checkJob();
		if(run){
			parallelRun(jobType);
		}
	}
	
	/**
	 * 根据跑批标志判断是否跑批
	 * （当前未实现）
	 * @return
	 */
	public boolean checkParam(){
		return true;
	}
	
	/**
	 * 初始化任务运行状态
	 * @return
	 */
	public boolean initJobStatus(){
		Connection conn = null;
		boolean result = false;
		try{	
			conn = DataSourceManager.getConnection();			
			PreparedStatement ps = 
					conn.prepareStatement("UPDATE ETL.EDW_JOB_STATUS "
							+ "SET STATUS=NULL "
							+ "WHERE DATA_DATE=? AND STATUS <>'DONE';");
			ps.setString(1, vDataDate);
			result=ps.execute();
			ps.close();
			System.out.println("INFO "+"Success to refresh the table[EDW_JOB_STATUS] status successfully.");
		}catch(Exception e){
			e.printStackTrace();
			System.out.println("ERROR "+"Fail to refresh the status.");
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
	 * 检查任务数量
	 * @return
	 */
	public boolean checkJob(){
		Connection conn = null;
		boolean runFlag = true;
		String jobCount="0";
		try{
			conn = DataSourceManager.getConnection();			
			PreparedStatement ps = 
					conn.prepareStatement("SELECT COALESCE(COUNT(1),0) "
							+ "FROM ETL.EDW_JOB_STATUS "
							+ "WHERE DATA_DATE=? AND STATUS IS NULL");
			ps.setString(1, vDataDate);
			ResultSet resultSet=ps.executeQuery();
			if(resultSet.next()){
				jobCount = resultSet.getString(1);
				if(jobCount.equalsIgnoreCase("0")){
					runFlag = false;
					System.out.println("WARN "+"No job to run.");
				}else{
					System.out.println("INFO "+"Has "+jobCount+" job to run.");
				}
			}
		}catch(Exception e){
			e.printStackTrace();
			System.out.println("ERROR"+"Fail to get run job num.");
			System.out.println("DEBUG"+"DATA_DATE is ："+vDataDate);
		}finally{
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
		}
		return runFlag;
	}
	
	/**
	 * 并行执行
	 * @param jobType
	 * @return
	 */
	public boolean parallelRun(String jobType){
		boolean correct = true;
		JobTask jt = new JobTask(vDataDate,jobType,parallelNum,parallelNum,true);
		Integer count = new ForkJoinPool().invoke(jt);
		if(count<parallelNum){
			System.out.println("ERROR "+"Fail to run the procedure.");
			correct = false;
		}else{
			System.out.println("INFO "+"All jobs have done.");
		}
		return correct;
	}

}
