package com.NSFC.operator.hbase.exception;

public class TableNotFoundsException extends Exception {

	private static final long serialVersionUID = 1L;
	
	public TableNotFoundsException()
	{
		super();
	}
	
	public TableNotFoundsException(String tableName)
	{
		super("Table : \"" + tableName + "\" Not Found!");
	}
}
