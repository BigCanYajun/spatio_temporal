package com.NSFC.operator.hbase.exception;

public class FindNoDataException extends Exception{

	private static final long serialVersionUID = 1L;

	public FindNoDataException() {
		super();
	}

	public FindNoDataException(String rowKey) {
		super("RowKey : \"" + rowKey + "\" Find No Data!");
	}
	
	public FindNoDataException(String rowKey, String colFamily,
			String qualifier) {
		super("RowKey : \"" + rowKey + "\", Column Family : \"" + colFamily
				+ "\", Qualifier : \"" + qualifier + "\" Find No Data!");
	}
}
