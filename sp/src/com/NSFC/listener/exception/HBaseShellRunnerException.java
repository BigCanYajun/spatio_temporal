package com.NSFC.listener.exception;

public class HBaseShellRunnerException extends Exception {

	private static final long serialVersionUID = 1L;
	
	public HBaseShellRunnerException()
	{
		super("Startup HBase shell error!");
	}
}
