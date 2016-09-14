package com.NSFC.operator.hbase.operator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.NSFC.operator.hbase.exception.FindNoDataException;
import com.NSFC.operator.hbase.exception.TableAlreadyExistsException;
import com.NSFC.operator.hbase.exception.TableNotFoundsException;

public class HBaseOperator {
	
	private static Logger logger = LoggerFactory.getLogger(HBaseOperator.class);

	public static Connection connection;
	public static Admin admin;

	public static Admin getAdmin() {
		return admin;
	}

	public static void setAdmin(Admin admin) {
		HBaseOperator.admin = admin;
	}

	public static Connection getConnection() {
		return connection;
	}

	public static void setConnection(Connection connection) {
		HBaseOperator.connection = connection;
	}
	
	private String pictureFilePath;		//Í¼Æ¬ÎÄ¼þÂ·¾¶
	
	public void createTable(String tableName) throws TableAlreadyExistsException {
		TableName table = TableName.valueOf(tableName);
		try {
			if (admin.tableExists(table)) {
				throw new TableAlreadyExistsException(tableName);
			} else {
				HTableDescriptor tableDes = new HTableDescriptor(table);
				HColumnDescriptor hColumnDes = new HColumnDescriptor("info");
				tableDes.addFamily(hColumnDes);
				
				admin.createTable(tableDes);
				logger.info("Create table : \"" + tableName + "\" success!");
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void deleteTable(String tableName) throws TableNotFoundsException {
		TableName table = TableName.valueOf(tableName);
		try {
			if (admin.tableExists(table)) {
				admin.disableTable(table);
				admin.deleteTable(table);
				logger.info("Delete table : \"" + tableName + "\" success!");
			} else {
				throw new TableNotFoundsException(tableName);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public String[] listTables() {
		HTableDescriptor hTableDescriptors[];
		String[] tablesArr = null;
		try {
			hTableDescriptors = admin.listTables();
			tablesArr = new String[hTableDescriptors.length];
			for(int i = 0; i < hTableDescriptors.length; i++) {
				tablesArr[i] = hTableDescriptors[i].getNameAsString();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return tablesArr;
	}
	
	public Result getData(String tableName, String rowkey) throws TableNotFoundsException {
		Result result = null;
		
		TableName userTable = TableName.valueOf(tableName);
		Get get = new Get(rowkey.getBytes());
		
		try {
			if(admin.tableExists(userTable)) {
				Table table = connection.getTable(userTable);
				result = table.get(get);
			} else {
				throw new TableNotFoundsException(tableName);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return result;
	}
	
	public Result getData(String tableName, String rowkey, String colFamily,String qualifier) throws FindNoDataException, TableNotFoundsException{
		Result result = null;
		
		TableName userTable = TableName.valueOf(tableName);
		Get get = new Get(rowkey.getBytes());
		
		try {
			if (admin.tableExists(userTable)) {
				Table table = connection.getTable(userTable);
				result = table.get(get);
				if (!result.isEmpty()) {
					get.addColumn(colFamily.getBytes(), qualifier.getBytes());
					result = table.get(get);
					if (!result.isEmpty()) {
						return result;
					} else {
						throw new FindNoDataException(rowkey, colFamily, qualifier);
					}
				} else {
					throw new FindNoDataException(rowkey);
				}
			} else {
				throw new TableNotFoundsException(tableName);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return result;
	}
	
	public Result getData(String tableName, String rowkey, String colFamily, ArrayList<String> qualifiers) throws FindNoDataException, TableNotFoundsException {
		Result result = null;

		TableName userTable = TableName.valueOf(tableName);
		Get get = new Get(rowkey.getBytes());
		
		try {
			if (admin.tableExists(userTable)) {
				Table table = connection.getTable(userTable);
				
				result = table.get(get);
				String qualifier;
				if (!result.isEmpty()) {
					for (int i = 0; i < qualifiers.size(); i++) {
						qualifier = qualifiers.get(i);
						get.addColumn(colFamily.getBytes(), qualifier.getBytes());
					}
					result = table.get(get);
				} else {
					throw new FindNoDataException(rowkey);
				}
			} else {
				throw new TableNotFoundsException(tableName);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return result;
	}
	
	public ArrayList<Result> getData(String tableName, String startRowKey, String stopRowKey) throws TableNotFoundsException {
		ArrayList<Result> resultArray = new ArrayList<Result>();
		TableName userTable = TableName.valueOf(tableName);
		Scan scan = new Scan();
		scan.setStartRow(startRowKey.getBytes());
		scan.setStopRow(stopRowKey.getBytes());
		
		try {
			if(admin.tableExists(userTable)) {
				Table table = connection.getTable(userTable);
				ResultScanner rs = table.getScanner(scan);

				for(Result result : rs) {
					resultArray.add(result);
				}
			} else {
				throw new TableNotFoundsException(tableName);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return resultArray;
	}
	
	public void deleteData(String tableName, String rowkey) throws TableNotFoundsException, FindNoDataException {
		logger.info("Delete table : \"" + tableName + "\" -- row : \"" + rowkey + "\"......");
		
		TableName userTable = TableName.valueOf(tableName);
		
		try {
			if(admin.tableExists(userTable)){
				Get get = new Get(rowkey.getBytes());
				Table table = connection.getTable(TableName.valueOf(tableName));
				Result result = table.get(get);
				if(!result.isEmpty()) {
					pictureFilePath = new String(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("PicturePath")));
					Delete delete = new Delete(Bytes.toBytes(rowkey));
					table.delete(delete);
					logger.info("Delete table : \"" + tableName + "\" --  row : \"" + rowkey + "\" success!");
				} else {
					throw new FindNoDataException(rowkey);
				}
				
				table.close();
			} else {
				throw new TableNotFoundsException(tableName);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void deleteData(String tableName, String rowkey, String colFamily, String qualifier) throws FindNoDataException, TableNotFoundsException {
		logger.info("Delete table : \"" + tableName + "\" -- row : \"" + rowkey + "\" -- qualifier : \"" + qualifier + "\"......");
		
		TableName userTable = TableName.valueOf(tableName);
		
		try {
			if(admin.tableExists(userTable)) {
				Get get = new Get(rowkey.getBytes());
				Table table = connection.getTable(TableName.valueOf(tableName));
				Result result = table.get(get);
				if (!result.isEmpty()) {
					get.addColumn(colFamily.getBytes(), qualifier.getBytes());
					result = table.get(get);
					
					if (!result.isEmpty()) {
						if (qualifier.equals("PicturePath")) {
							pictureFilePath = new String(result.getValue(Bytes.toBytes(colFamily), Bytes.toBytes("PicturePath")));
						}
						Delete delete = new Delete(Bytes.toBytes(rowkey));
						delete.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(qualifier));
						table.delete(delete);
						logger.info("Delete table : \"" + tableName + "\" -- row : \"" + rowkey + "\" -- qualifier : \"" + qualifier + "\"success!");
					} else {
						throw new FindNoDataException(rowkey,colFamily,qualifier);
					}
				} else {
					throw new FindNoDataException(rowkey);
				}
				
				table.close();
			} else {
				throw new TableNotFoundsException(tableName);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void uploadPut (Put put, String tableName, String colFamily) throws TableNotFoundsException {
		TableName userTable = TableName.valueOf(tableName);
		
		try {
			if (admin.tableExists(userTable)) {
				Table table = connection.getTable(userTable);
				table.put(put);
				table.close();
			} else {
				throw new TableNotFoundsException(tableName);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void showResult(Result result) {

		String rowName = "";
		String colFamilyName = "";

		Cell[] cells = result.rawCells();
		for (Cell cell : cells) {
			if (rowName.isEmpty()) {
				rowName = new String(CellUtil.cloneRow(cell));
				System.out.println("RowName: " + rowName);
			}

			if (colFamilyName.isEmpty()
					|| !colFamilyName.equals(new String(CellUtil
							.cloneFamily(cell)))) {
				colFamilyName = new String(CellUtil.cloneFamily(cell));
				System.out.println("	Column Family: " + colFamilyName);
			}

			System.out.println("		Qualifier Name: "
					+ new String(CellUtil.cloneQualifier(cell)) + ", Value: "
					+ new String(CellUtil.cloneValue(cell)));
		}
	}
}
