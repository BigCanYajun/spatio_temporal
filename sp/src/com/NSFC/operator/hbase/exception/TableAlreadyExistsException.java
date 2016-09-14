package com.NSFC.operator.hbase.exception;

public class TableAlreadyExistsException extends Exception {

private static final long serialVersionUID = 1L;
	
	public TableAlreadyExistsException()
	{
		super();
	}
	
	public TableAlreadyExistsException(String tableName)
	{
		super("Table : \"" + tableName + "\" is Already Exist!");
	}
}
