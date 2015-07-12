package com.customtime.data.conversion.demo.client;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.customtime.data.conversion.client.DataConversionClient;
import com.customtime.data.conversion.demo.client.util.FileUtil;

public class IptraceClient implements DataConversionClient{

	private static final Log logger = LogFactory.getLog(IptraceClient.class);
	private int port;
	private long sleepTime;
	private String rediusDir;
	private String rediusFileReg;
	private String redPlanPath;
	private String natDir;
	private String natFileReg;
	private String natPlanPath;
	private String dpiDir;
	private String dpiFileReg;
	private String dpiPlanPath;
	private String resultPath;
	private String dpiDelay;
	private String dpiRetryFileSuffix;
	private String dpiRetryDir;
	private String dbDriverString;
	private String dbConnUrl;
	private String dbUserName;
	private String dbUserPwd;
	private int min=-1;
	public IptraceClient(){
		Properties props = new Properties();
		try {
			props.load(this.getClass().getClassLoader().getResourceAsStream("iptrace.properties"));
			port = Integer.parseInt(props.getProperty("port", "7914"));
			sleepTime = Long.parseLong(props.getProperty("sleepTime", "3000"));
			rediusDir = props.getProperty("rediusDir");
			if(rediusDir != null && !rediusDir.endsWith(File.separator)){
				rediusDir = rediusDir+File.separator;
			}
			rediusFileReg = props.getProperty("rediusFileReg");
			redPlanPath = props.getProperty("redPlanPath");
			natDir = props.getProperty("natDir");
			if(natDir != null && !natDir.endsWith(File.separator)){
				natDir = natDir+File.separator;
			}
			natFileReg = props.getProperty("natFileReg");
			natPlanPath = props.getProperty("natPlanPath");
			dpiDir = props.getProperty("dpiDir");
			if(dpiDir != null && !dpiDir.endsWith(File.separator)){
				dpiDir = dpiDir+File.separator;
			}
			dpiFileReg = props.getProperty("dpiFileReg");
			dpiPlanPath = props.getProperty("dpiPlanPath");
			resultPath = props.getProperty("resultPath");
			if(resultPath != null && !resultPath.endsWith(File.separator)){
				resultPath = resultPath+File.separator;
			}
			dpiDelay = props.getProperty("dpiDelay","");
			if(dpiDelay!=null && !"".equals(dpiDelay)){
				try {
					min = Integer.parseInt(dpiDelay);
				} catch (NumberFormatException e) {
					logger.error("the property dpiDelay is must be number!");
					e.printStackTrace();
				}
				
			}
			dpiRetryFileSuffix = props.getProperty("dpiRetryFileSuffix",".retry1");
			if(dpiRetryFileSuffix != null && !dpiRetryFileSuffix.startsWith(".")){
				dpiRetryFileSuffix = "."+dpiRetryFileSuffix;
			}
			
			dpiRetryDir = props.getProperty("dpiRetryDir");
			if(dpiRetryDir==null || "".equals(dpiRetryDir = dpiRetryDir.trim())){
				dpiRetryDir = dpiDir + "dpiretry";
			}
			if(dpiRetryDir != null && !dpiRetryDir.endsWith(File.separator)){
				dpiRetryDir = dpiRetryDir+File.separator;
			}
			
			dbDriverString = props.getProperty("dbDriverString");
			dbConnUrl = props.getProperty("dbConnUrl");
			dbUserName = props.getProperty("dbUserName");
			dbUserPwd = props.getProperty("dbUserPwd");
		} catch (IOException e) {
			logger.error("there have not iptrace.properties!" );
			e.printStackTrace();
		}
	}
	public void start() {
		logger.info("NIOAccptor Clinet start,please input message with line!");
		SocketChannel client = null;
		ByteBuffer byteBuffer = ByteBuffer.allocate(1024*10);
		boolean allNull = true;
		String command = "";
		try {
			Thread.sleep(2000);//为防止操作过快（TE还没起好），先等待2秒钟
			String dbCommand = getDbCommand("HANDLERCHECK");
			ScanningFile redScan = new ScanningFile(rediusDir,rediusFileReg,"HANDLERCHECK");
			ScanningFile natScan = new ScanningFile(natDir,natFileReg,"HANDLERCHECK");
			ScanningFile dpiScan = new ScanningFile(dpiDir,dpiFileReg,"HANDLERCHECK");
			CharsetEncoder encoder = Charset.forName("utf8").newEncoder();
			client = SocketChannel.open();
			client.configureBlocking(false);
			client.connect(new InetSocketAddress(port));
			if(client.isConnectionPending())
				client.finishConnect();
			while(true){
				//String redFile = redScan.scanning();
				ScannedFileInfo redScannedInfo = redScan.scanning();
				if(redScannedInfo!=null){
					String redFile = redScannedInfo.getScannedFileName();
					String redExCommand = redScannedInfo.getExCommand();
					command="dealPlan proPlanPath="+redPlanPath+" RCLID.filePath="+rediusDir+File.separator+redFile;
					command += redExCommand + dbCommand+" ";
					client.write(encoder.encode(CharBuffer.wrap(command)));
					byteBuffer.clear();
					Thread.sleep(1000);
					allNull = false;
				}
				//String natFile = natScan.scanning();
				ScannedFileInfo natScannedInfo = natScan.scanning();
				if(natScannedInfo!=null){
					String natFile = natScannedInfo.getScannedFileName();
					String natExCommand = natScannedInfo.getExCommand();
					command="dealPlan proPlanPath="+natPlanPath+" RCLID.fileName="+natFile+" RCLID.fileDir="+natDir;
					command += natExCommand + dbCommand +" ";
					client.write(encoder.encode(CharBuffer.wrap(command)));
					byteBuffer.clear();
					Thread.sleep(1000);
					allNull = false;
				}
				//String dpiFile = dpiScan.scanning();
				ScannedFileInfo dpiScannedInfo = dpiScan.scanning();
				if(dpiScannedInfo!=null){
					String dpiFile = dpiScannedInfo.getScannedFileName();
					String dpiExCommand = dpiScannedInfo.getExCommand();
					if(min>0){
						dpiDelay = " cronSchedule="+ getCronTime(min,TimeUnit.MINUTES);
					}else{
						dpiDelay = "";
					}
					command="dealPlan proPlanPath="+dpiPlanPath+" RCLID.fileName="+dpiFile+" RCLID.fileDir="+dpiDir+" wid2.fileName="+dpiFile+".txt wid2.fileDir="+resultPath+dpiDelay;
					command += dpiExCommand + dbCommand;
					
					/*dpi定时执行计划的线程插件只需给校验处理插件传入正确的:
					HANDLERCHECK.orgFilePath（当前文件的全路径） 
					HANDLERCHECK.dpiOrgFileSuffix（原始文件的文件名后缀）
					HANDLERCHECK.dpiOrgFileDir（原始文件所在的文件夹目录）
					HANDLERCHECK.dpiOrgResultFilePath（原始文件生成的结果文件全路径）
					另外还需要传入原始文件执行时相同的proPlanPath、wid2.fileDir、wid2.fileName，并且wid2.fileAppend=true
					其它的命令：
					RCLID.fileName RCLID.fileDir以扫描到的重关联文件为准
					hid_raduis_ehcache.errFile hid_nat_ehcache.errFile 必须相同且指向的是下一个定时执行计划插件可以扫描到的重关联文件规则
					HANDLERCHECK.dpiRetryTime 为重关联的次数（1、2、3）
					
					
					线程插件的配置项应该要包括：
					iptrace.properties中的dpiDir（即HANDLERCHECK.dpiOrgFileDir）、dpiRetryFileSuffix、dpiRetryDir
					*/
					String dpiOrgFileSuffix = dpiFile.substring(dpiFile.lastIndexOf("."));
					String dpiOrgFileDir = dpiDir;
					String dpiFileWithOutSuffix = dpiFile.substring(0,dpiFile.lastIndexOf("."));
					FileUtil.createDir(dpiRetryDir);
					String dpiRetryFilePath = dpiRetryDir + dpiFileWithOutSuffix + dpiRetryFileSuffix;//这步很重要，要保证重关联时的文件名与原始文件名相同，以便后续处理
					String dpiRetryFileCommand = " hid_raduis_ehcache.errFile="+dpiRetryFilePath + " hid_nat_ehcache.errFile="+dpiRetryFilePath;
					String dpiOrgFileCommand = " HANDLERCHECK.dpiOrgFileSuffix="+dpiOrgFileSuffix + " HANDLERCHECK.dpiOrgFileDir="+dpiOrgFileDir +" HANDLERCHECK.dpiOrgResultFilePath="+resultPath+dpiFile+".txt";
					command += dpiRetryFileCommand + dpiOrgFileCommand +" HANDLERCHECK.dpiRetryTime=0 ";
					
					client.write(encoder.encode(CharBuffer.wrap(command)));
					byteBuffer.clear();
					Thread.sleep(1000);
					allNull = false;
				}
				if(allNull)
					Thread.sleep(sleepTime);
				allNull=true;
			}
		} catch (CharacterCodingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch ( InterruptedException e) {
			e.printStackTrace();
		}finally{
			if(client!=null)
				try {
					client.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
		}
	}

	public void stop() {
		SocketChannel client = null;
		try {
			CharsetEncoder encoder = Charset.forName("utf8").newEncoder();
			client = SocketChannel.open();
			client.configureBlocking(false);
			client.connect(new InetSocketAddress(port));
			if(client.isConnectionPending())
				client.finishConnect();
			client.write(encoder.encode(CharBuffer.wrap("exitTE")));
		} catch (CharacterCodingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			if(client!=null)
				try {
					client.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
		}
	}
	
	
	private String getDbCommand(String mainClassId){
		StringBuffer sb = new StringBuffer();
		sb.append(" "+mainClassId+".dbDriverString="+dbDriverString);
		sb.append(" "+mainClassId+".dbConnUrl="+dbConnUrl);
		sb.append(" "+mainClassId+".dbUserName="+dbUserName);
		sb.append(" "+mainClassId+".dbUserPwd="+dbUserPwd);
		return sb.toString();
	}
	
	class ScanningFile{
		String dir;
		String fileReg;
		String fileSuffix;
		String findFileReg;
		String findFileSuffix=".ok";
		ScannedFileInfo scannedFileInfo;
		ScanningFile(String dir,String fileReg,String mainClassId){
			this.scannedFileInfo = new ScannedFileInfo(mainClassId,dir);
			this.dir=dir;
			this.fileReg=fileReg;
			this.fileSuffix = fileReg.substring(fileReg.lastIndexOf("."));
			//this.findFileReg = fileReg.substring(0,fileReg.lastIndexOf("."))+this.findFileSuffix;
			this.findFileReg = fileReg+"\\"+this.findFileSuffix;
		}
		
		ScannedFileInfo scanning(){
			File flieDir = new File(dir);
			if(!flieDir.isDirectory())
				return null;
			if(dir != null && !dir.endsWith(File.separator))
				dir = dir+File.separator;
			String[] dirFiles = flieDir.list();
			
			for(String dfs : dirFiles){
				/*if(!dfs.endsWith(".scand")&&dfs.matches(fileReg)){
					scannedFileInfo.setFileName(dfs);
					scannedFileInfo.makeExCommand();
					String newName = dfs+".scand";
					if(new File(dir+dfs).renameTo(new File(dir+newName))){
						scannedFileInfo.setScannedFileName(newName);
						return scannedFileInfo;
					}
						
				}*/
				if(new File(dir+dfs).isFile()&&dfs.endsWith(findFileSuffix)&&dfs.matches(findFileReg)){
					scannedFileInfo.setFileName(dfs);
					String newName = dfs+".scand";
					//String orgName = dfs.substring(0,dfs.lastIndexOf(findFileSuffix))+fileSuffix;
					String orgName = dfs.substring(0,dfs.lastIndexOf(findFileSuffix));
					scannedFileInfo.setScannedFileName(orgName);
					scannedFileInfo.makeExCommand();
					if(new File(dir+dfs).renameTo(new File(dir+newName))){
						File file = new File(dir+orgName);
						if(file.exists()){
							return scannedFileInfo;
						}
					}
						
				}
			}
			return null;
		}
		
	}
	
	class ScannedFileInfo{
		private String scannedFileName;
		private String mainClassId;
		private String fileDir;
		private String fileName;
		private String exCommand;
		
		public String getScannedFileName() {
			return scannedFileName;
		}
		public void setScannedFileName(String scannedFileName) {
			this.scannedFileName = scannedFileName;
		}
		public String getMainClassId() {
			return mainClassId;
		}
		public void setMainClassId(String mainClassId) {
			this.mainClassId = mainClassId;
		}
		public String getFileDir() {
			return fileDir;
		}
		public void setFileDir(String fileDir) {
			this.fileDir = fileDir;
		}
		public String getFileName() {
			return fileName;
		}
		public void setFileName(String fileName) {
			this.fileName = fileName;
		}
		public String getExCommand() {
			return this.exCommand;
		}
		public void setExCommand(String exCommand) {
			this.exCommand = exCommand;
		}
		public ScannedFileInfo(String mainClassId,String fileDir){
			this.mainClassId = mainClassId;
			this.fileDir = fileDir;
		}
		public ScannedFileInfo(String mainClassId,String fileDir,String fileName){
			this.mainClassId = mainClassId;
			this.fileDir = fileDir;
			this.fileName = fileName;
		}
		/*public Map<String,String> getExCommand(String[] mainClassIds,String[] fileDirs,String[] fileNames){
			Map<String,String> map = new HashMap<String,String>();
			String filePath;
			String fileSize;
			String fileCollectTime;
			String fileParseTime = getFileParseTime();
			String mainClassId;
			String fileDir;
			String fileName;
			for(int i=0;i<mainClassIds.length;i++){
				StringBuffer sb = new StringBuffer();
				mainClassId = mainClassIds[i];
				fileDir = fileDirs[i];
				fileName = fileNames[i];
				filePath = getFilePath(fileDir,fileName);
				fileSize = getFileSize(filePath);
				fileCollectTime = getFileCollectTime(filePath);
				
				sb.append(" "+mainClassId+".filePath="+filePath);
				sb.append(" "+mainClassId+".fileSize="+fileSize);
				sb.append(" "+mainClassId+".fileCollectTime="+fileCollectTime);
				sb.append(" "+mainClassId+".fileParseTime="+fileParseTime);
				map.put(mainClassId, sb.toString());
				
			}
			return map;
		}*/
		
		public void makeExCommand(){
			StringBuffer sb = new StringBuffer();
			String orgFilePath = getFilePath(fileDir,scannedFileName);
			String fileSize = getFileSize(orgFilePath);
			String fileCollectTime = getFileCollectTime(orgFilePath);
			String fileParseTime = getFileParseTime();
			
			sb.append(" "+mainClassId+".orgFilePath="+orgFilePath);
			sb.append(" "+mainClassId+".fileSize="+fileSize);
			sb.append(" "+mainClassId+".fileCollectTime="+fileCollectTime);
			sb.append(" "+mainClassId+".fileParseTime="+fileParseTime);
			this.exCommand = sb.toString();
				
		}
		private String getFilePath(String fileDir,String fileName){
			if(fileDir != null && !fileDir.endsWith(File.separator)){
				fileDir = fileDir+File.separator;
				this.fileDir = fileDir;
			}
			return fileDir+fileName;
		}
		private String getFileSize(String filePath){
			long size = FileUtil.getFileSize(filePath, 1048576);
			//return String.valueOf(size)+"MB";
			return String.valueOf(size);
		}
		private String getFileCollectTime(String filePath){
			String time = FileUtil.getFileLastModifiedTime(filePath, "yyyyMMddHHmmss");
			return time;
		}
		private String getFileParseTime(){
			Date date = new Date();
			SimpleDateFormat formatter = new SimpleDateFormat ("yyyyMMddHHmmss");
			return formatter.format(date);
		}
	}
	
	public static String getCronTime(int time,TimeUnit tu){
    	long now = System.currentTimeMillis();
    	long then = now + tu.toMillis(time);
    	Calendar thenTime = Calendar.getInstance();
    	thenTime.setTimeInMillis(then);
    	StringBuffer sb = new StringBuffer(thenTime.get(Calendar.SECOND)+"").append(",").append(thenTime.get(Calendar.MINUTE)).append(",").append(thenTime.get(Calendar.HOUR_OF_DAY)).append(",").append(thenTime.get(Calendar.DATE)).append(",").append(thenTime.get(Calendar.MONTH)+1).append(",?,").append(thenTime.get(Calendar.YEAR));
    	return sb.toString();
    }
}
