package com.uniplore.executor;
/**
 * SQL执行器接口
 * @author tian
 *
 */
public interface ExecutorInf {
	void runSQL(String sql);
	void runFile();
	void exportData();
}
