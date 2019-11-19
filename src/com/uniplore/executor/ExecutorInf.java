package com.uniplore.executor;
/**
 * SQL执行器接口
 * @author tian
 *
 */
public interface ExecutorInf {
	/**
	 * 执行SQL语句
	 * @param sql
	 */
	void runSQL(String sql);
	/**
	 * 执行SQL文件
	 * @param path
	 */
	void runFile(String path,String param);
	void exportData();
	/**
	 * 执行存储过程
	 * @param procName
	 * @param param
	 */
	void runProc(String procName,String param);
}
