package com.uniplore;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RecursiveTask;

import com.uniplore.tools.ProcLauncher;

/**
 * 并行任务类
 * @author tian
 *
 */
public class JobTask extends RecursiveTask<Integer> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1992163893544802526L;
	
	private String jobType;
	private int num;
	private int index;
	private boolean isRoot;
	private String vDataDate;
	
	/**
	 * 
	 * @param date 日期
	 * @param jobType 任务类型
	 * @param index 序号
	 * @param num 并行数量
	 * @param isRoot 
	 */
	public JobTask(String date,String jobType,int index,int num,boolean isRoot) {
		this.jobType = jobType;
		this.num = num;
		this.isRoot = isRoot;
		this.index = index;
		this.vDataDate = date;
	}

	@Override
	protected Integer compute() {
		int count=0;
		List<JobTask> subJobTasks = new ArrayList<JobTask>();
		if(isRoot){
			for(int i=0;i<num;i++){
				subJobTasks.add(new JobTask(vDataDate,this.jobType,i,num,false));
			}
		}else{
			try{
				ProcLauncher procLauncher = new ProcLauncher(vDataDate, jobType, index, num);
				procLauncher.startWork();
				count++;
			}catch(Exception e){
				count=0;
			}
			
		}
		
		
		if(!subJobTasks.isEmpty()){
			for(JobTask subTask:invokeAll(subJobTasks)){
				count+=subTask.join();
			}
		}
		return count;
	}

}
