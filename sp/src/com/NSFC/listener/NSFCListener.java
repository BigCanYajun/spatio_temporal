package com.NSFC.listener;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.NSFC.listener.exception.HBaseCommandPathException;
import com.NSFC.listener.exception.HBaseShellRunnerException;
import com.NSFC.listener.exception.HadoopCommandPathException;
import com.NSFC.listener.exception.HadoopSafemodeException;
import com.NSFC.listener.exception.HadoopShellRunnerException;
import com.NSFC.operator.hbase.operator.HBaseOperator;

public class NSFCListener implements ServletContextListener {

	private static Logger logger = LoggerFactory.getLogger(NSFCListener.class);
	
	private static Configuration configuration;
	private static Connection connection;
	private static Admin admin;
	
	private static String hadoopHome;
	private static String hbaseHome;
	
	@Override
	public void contextDestroyed(ServletContextEvent arg0) {
		// TODO Auto-generated method stub
		shutdownHBaseClientConnection();
		shutdownShell();
	}

	@Override
	public void contextInitialized(ServletContextEvent arg0) {
		// TODO Auto-generated method stub
		try {
			startupShell();
		} catch (HadoopSafemodeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		startupHBaseClientConnection();
	}
	
	private void startupShell() throws HadoopSafemodeException {
		logger.info("Startup...");
		try {
			this.startupHadoopShell();
			String safemodeCommandPath = hadoopHome + "/bin/hadoop dfsadmin -safemode get";
			boolean isSafemodeOff = this.isSafemodeOFF(safemodeCommandPath);
			if (isSafemodeOff == false) {
				throw new HadoopSafemodeException();
			} else {
				this.startupHBaseShell();
			}
		} catch (HadoopShellRunnerException | HadoopCommandPathException | HBaseCommandPathException | HBaseShellRunnerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void shutdownShell() {
		logger.info("Shutdown...");
		try {
			this.shutdownHBaseShell();
			this.shutdownHadoopShell();
			
		} catch (HBaseCommandPathException | HBaseShellRunnerException | HadoopCommandPathException | HadoopShellRunnerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void startupHadoopShell() throws HadoopShellRunnerException, HadoopCommandPathException {
		logger.info("Hadoop startup...");
		hadoopHome = System.getenv("HADOOP_HOME");
		String hadoopCommandPath =  hadoopHome + "/sbin/start-all.sh";
		File hadoopCommandFile = new File(hadoopCommandPath);
		if (!hadoopCommandFile.exists()) {
			throw new HadoopCommandPathException();
		}
		int exitValue = this.shellRunner(hadoopCommandPath);
		if (exitValue != 0) {
			throw new HadoopShellRunnerException();
		} else {
			logger.info("Hadoop started..");
		}
	}
	
	private void shutdownHadoopShell() throws HadoopCommandPathException, HadoopShellRunnerException {
		logger.info("Hadoop shutdown...");
		hadoopHome = System.getenv("HADOOP_HOME");
		String hadoopCommandPath = hadoopHome + "/sbin/stop-all.sh";
		File hadoopCommandFile = new File(hadoopCommandPath);
		if (!hadoopCommandFile.exists()){
			throw new HadoopCommandPathException();
		}
		int exitValue = this.shellRunner(hadoopCommandPath);
		if (exitValue != 0){
			throw new HadoopShellRunnerException();
		} else {
			logger.info("Hadoop stoped...");
		}
	}
	
	private void startupHBaseShell() throws HBaseCommandPathException, HBaseShellRunnerException {
		logger.info("HBase startup...");
		hbaseHome = System.getenv("HBASE_HOME");
		String hbaseCommandPath = hbaseHome + "/bin/start-hbase.sh";
		File hbaseCommandFile = new File(hbaseCommandPath);
		if (!hbaseCommandFile.exists()) {
			throw new HBaseCommandPathException();
		}
		int exitValue = this.shellRunner(hbaseCommandPath);
		if (exitValue != 0) {
			throw new HBaseShellRunnerException();
		} else {
			logger.info("HBase started...");
		}
		
	}
	
	private void shutdownHBaseShell() throws HBaseCommandPathException, HBaseShellRunnerException {
		logger.info("HBase shutdown...");
		hbaseHome = System.getenv("HBASE_HOME");
		String hbaseCommandPath = hbaseHome + "/bin/stop-hbase.sh";
		File hbaseCommandFile = new File(hbaseCommandPath);
		if (!hbaseCommandFile.exists()) {
			throw new HBaseCommandPathException();
		}
		int exitValue = this.shellRunner(hbaseCommandPath);
		if (exitValue != 0) {
			throw new HBaseShellRunnerException();
		} else {
			logger.info("HBase stoped...");
		}
	}
	
	private int shellRunner(String commandPath) {
		Runtime runtime = Runtime.getRuntime();
		int exitValue = 1;
		try {
			Process pcs = runtime.exec(commandPath);
			BufferedReader br = new BufferedReader(new InputStreamReader(pcs.getInputStream()));
			String line = new String();
			while ((line = br.readLine()) != null) {
				logger.info(line);
			}
			pcs.waitFor();
			br.close();
			exitValue = pcs.exitValue();
			logger.info("Exit Value is : " + exitValue);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return exitValue;
	}
	
	private boolean isSafemodeOFF (String commandPath) throws HBaseShellRunnerException {
		boolean isSafemodeOff = false;
		Runtime runtime = Runtime.getRuntime();
		Process pcs;
		BufferedReader br;
		int exitValue = 1;
		try {
			String line = "";
			int sleepTime = 0;
			while (!line.contains("OFF")) {
				pcs = runtime.exec(commandPath);
				br = new BufferedReader(new InputStreamReader(pcs.getInputStream()));
				line = br.readLine();
				logger.info(line);
				pcs.waitFor();
				br.close();
				exitValue = pcs.exitValue();
				if (exitValue != 0) {
					throw new HBaseShellRunnerException();
				}
				if(!line.contains("OFF")){
					logger.info("Sleep 1500ms...");
					Thread.sleep(1500);
					sleepTime += 1500;
					if(sleepTime > 30000) {
						logger.info("Safemode is ON overtime!");
						break;
					}
				} else {
					isSafemodeOff = true;
				}
			}
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return isSafemodeOff;
	}
	
	private void startupHBaseClientConnection() {
		configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.property.clientPort", "2181");
		configuration.set("hbase.zookeeper.quorum", "192.168.1.2");
		
		try {
			connection = ConnectionFactory.createConnection(configuration);
			admin = connection.getAdmin();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		HBaseOperator.setConnection(connection);
		HBaseOperator.setAdmin(admin);
		logger.info("HBase client connection...");
	}
	
	private void shutdownHBaseClientConnection() {
		admin = HBaseOperator.getAdmin();
		connection = HBaseOperator.getConnection();
		try {
			if (null != admin) {
				admin.close();
			}
			if (null != connection) {
				connection.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		logger.info("HBase client shutdown connection...");
	}

//	private void 
}
