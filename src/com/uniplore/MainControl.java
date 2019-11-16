package com.uniplore;

import com.uniplore.tools.ProcParallelLauncher;

public class MainControl {

	public MainControl() {
	
	}

	public static void main(String[] args) {
		String batchDate = "20170102";
		BatchStart bs = new BatchStart(batchDate);
		bs.start();
		
		ProcParallelLauncher ppl = new ProcParallelLauncher(batchDate, "GP", 1);
		ppl.start();
		
		BatchEnd be = new BatchEnd(batchDate);
		be.start();
		
	}

}
