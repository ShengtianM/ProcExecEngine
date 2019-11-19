package com.uniplore.executor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.uniplore.tools.DataSourceManager;
/**
 * GP SQL执行器
 * @author tian
 *
 */
public class GreenplumExecutor implements ExecutorInf {

	public GreenplumExecutor() {
	}

	@Override
	public void runFile(String path,String param) {

	}

	@Override
	public void exportData() {

	}

	@Override
	public void runSQL(String sql) {
		Connection conn = null;
		try{			
			conn = DataSourceManager.getConnection();			
			PreparedStatement ps = 
					conn.prepareStatement(sql);
			ps.executeQuery();
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

	@Override
	public void runProc(String procName, String param) {
		Connection conn = null;
		try{	
			conn = DataSourceManager.getConnection();			
			PreparedStatement ps = 
					conn.prepareStatement("SELECT "+ procName+"(?)");
			ps.setString(1, param);
			ps.execute();
			ps.close();
		}catch(Exception e){
			e.printStackTrace();
			System.out.println("ERROR"+" Fail to update the status.");
			System.out.println("DEBUG"+" DATA_DATE is ："+param);
		}finally{
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
		}
		
	}
}
