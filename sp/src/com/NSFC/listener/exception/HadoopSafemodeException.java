package com.NSFC.listener.exception;

public class HadoopSafemodeException extends Exception {

private static final long serialVersionUID = 1L;
	
	public HadoopSafemodeException()
	{
		super("Hadoop Safemode error!");
	}
}
