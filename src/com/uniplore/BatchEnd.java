package com.uniplore;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import com.uniplore.tools.DataSourceManager;

public class BatchEnd {

	private String vDataDate;
	public BatchEnd(String vDataDate) {
		this.vDataDate = vDataDate;
	}
	
	public void start(){
		System.out.println("INFO "+"Today batch date is "+vDataDate);
		updateBatchStatus();
		prepareNextBatchParam();
	}
	
	public void checkParam(String vDataDate){
		this.vDataDate=vDataDate;
	}
	
	public boolean updateBatchStatus(){
		Connection conn = null;
		boolean result = false;
		try{			
			conn = DataSourceManager.getConnection();			
			PreparedStatement ps = conn.prepareStatement("UPDATE ETL.EDW_BATCH_DATE SET STATUS='DONE' "
					+ "WHERE DATA_DATE=? AND STATUS='RUN';"
					+ "UPDATE ETL.EDW_BATCH_DATE SET END_TIME=to_char(NOW(),'yyyy-mm-dd hh:mm:ss') "
					+ "WHERE DATA_DATE=?;"
					+ "UPDATE ETL.EDW_BATCH_DATE "
					+ "SET RUN_TIME=NULL "
					+ "WHERE DATA_DATE=?");
			ps.setString(1, vDataDate);
			ps.setString(2, vDataDate);
			ps.setString(3, vDataDate);
			result=ps.execute();
			ps.close();
		}catch(Exception e){
			e.printStackTrace();
			System.out.println("ERROR"+"Can not update END_TIME.");
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
	
	public boolean prepareNextBatchParam(){
		Connection conn = null;
		boolean result = false;
		String nextDate = null;
		SimpleDateFormat sdf =   new SimpleDateFormat("yyyyMMdd");
		try{	
			Date date= sdf.parse(vDataDate);
			Calendar c = Calendar.getInstance();  
            c.setTime(date);  
            c.add(Calendar.DAY_OF_MONTH, 1);
            date = c.getTime();
			nextDate = sdf.format(date);
			
			conn = DataSourceManager.getConnection();			
			PreparedStatement ps = 
					conn.prepareStatement("DELETE FROM ETL.EDW_BATCH_DATE "
							+ "WHERE DATA_DATE=?;"
							+ "INSERT INTO ETL.EDW_BATCH_DATE(DATA_DATE) "
							+ "VALUES(?)");
			ps.setString(1, nextDate);
			ps.setString(2, nextDate);
			result=ps.execute();
			ps.close();
		}catch(Exception e){
			e.printStackTrace();
			System.out.println("ERROR"+"Can not prepare next batch.");
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
		return result;
	}

}
