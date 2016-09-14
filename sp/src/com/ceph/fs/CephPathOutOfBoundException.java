package com.ceph.fs;

public class CephPathOutOfBoundException extends Exception{

	 private static final long serialVersionUID = 1L;
	 
	 public CephPathOutOfBoundException() {
		 super();
	 }
	 
	 public CephPathOutOfBoundException(String path) {
		 super(path);
	 }
}
