package com.NSFC.listener.exception;

public class HadoopCommandPathException extends Exception {

	private static final long serialVersionUID = 1L;
	
	public HadoopCommandPathException()
	{
		super("Hadoop start command path error!");
	}
}
