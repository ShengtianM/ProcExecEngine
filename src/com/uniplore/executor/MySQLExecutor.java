package com.uniplore.executor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.uniplore.tools.DataSourceManager;
/**
 * MySQL SQL执行器，暂时未用到
 * @author tian
 *
 */
public class MySQLExecutor implements ExecutorInf {

	public MySQLExecutor() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public void runFile() {
		// TODO Auto-generated method stub

	}

	@Override
	public void exportData() {
		// TODO Auto-generated method stub

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

}
