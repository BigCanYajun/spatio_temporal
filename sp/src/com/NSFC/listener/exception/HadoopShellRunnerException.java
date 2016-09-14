package com.NSFC.listener.exception;

public class HadoopShellRunnerException extends Exception {

	private static final long serialVersionUID = 1L;
	
	public HadoopShellRunnerException()
	{
		super("Startup Hadoop shell error!");
	}
}
