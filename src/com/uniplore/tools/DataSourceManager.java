package com.uniplore.tools;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ResourceBundle;

import com.mchange.v2.c3p0.ComboPooledDataSource;

/**
 * 数据库连接池类
 * @author tian
 *
 */
public class DataSourceManager {

	public static ComboPooledDataSource dataSource=null;
	public DataSourceManager() {
		if(dataSource==null){
			ResourceBundle configuration=ResourceBundle.getBundle("com/uniplore/tools/config");
			dataSource=new ComboPooledDataSource();
			dataSource.setUser(configuration.getString("user"));
			dataSource.setPassword(configuration.getString("pwd"));
			dataSource.setJdbcUrl(configuration.getString("url"));
			try {
				dataSource.setDriverClass(configuration.getString("driver"));
			} catch (PropertyVetoException e) {
				e.printStackTrace();
			}
		}
		
	}
	
	/**
	 * 从连接池中取数据库连接
	 * @return
	 * @throws SQLException
	 */
	public static Connection getConnection() throws SQLException{
		if(dataSource==null){
			ResourceBundle configuration=ResourceBundle.getBundle("com/uniplore/tools/config");
			dataSource=new ComboPooledDataSource();
			dataSource.setUser(configuration.getString("user"));
			dataSource.setPassword(configuration.getString("pwd"));
			dataSource.setJdbcUrl(configuration.getString("url"));
			dataSource.setMinPoolSize(10);
			try {
				dataSource.setDriverClass(configuration.getString("driver"));
			} catch (PropertyVetoException e) {
				e.printStackTrace();
			}
		}
		return dataSource.getConnection();
	}
	
	

}
