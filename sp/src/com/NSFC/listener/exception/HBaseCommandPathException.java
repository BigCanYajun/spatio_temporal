package com.NSFC.listener.exception;

public class HBaseCommandPathException extends Exception {

	private static final long serialVersionUID = 1L;
	
	public HBaseCommandPathException()
	{
		super("HBase start command path error!");
	}
}
