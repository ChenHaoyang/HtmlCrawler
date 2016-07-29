package com.mad.HtmlCrawler;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
//import org.apache.hadoop.hbase.client.BufferedMutator;
//import org.apache.hadoop.hbase.client.Connection;
//import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
//import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Metadata;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.client.LaxRedirectStrategy;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.parser.Parser;
import org.mozilla.universalchardet.UniversalDetector;

/**
 * Hello world!
 *
 */
public class HtmlCrawler 
{
	private CloseableHttpClient m_httpClient;
	private RequestConfig m_requestConfig;
	private HttpHost m_httpHost;
	private UniversalDetector m_uDetector;
	public Configuration /*m_dfsConfig,*/ m_hbaseConfig;
	private String m_accessUser;
//	public FileSystem m_dfs;
	private String m_hdfsPath;
	private String m_osUserName;
	final private String m_proxyHost = "192.168.10.45";
	final private int m_proxyPort = 3128;
	final private int m_waitTime = 3000;
	
	public HtmlCrawler(String osUser, boolean proxy){
		if(osUser != null && !"".equals(osUser))
			m_osUserName = osUser;
		else
			m_osUserName = "charles";
		if(proxy){	
			m_httpHost = new HttpHost(m_proxyHost,m_proxyPort);
			m_httpClient = HttpClients.custom()
					.setRedirectStrategy(new LaxRedirectStrategy())
					.setProxy(m_httpHost)
					.build();
			m_requestConfig = RequestConfig.custom()
					.setSocketTimeout(m_waitTime)
					.setConnectTimeout(m_waitTime)
					.setConnectionRequestTimeout(m_waitTime)
					//.setProxy(m_httpHost)
					.build();
		}else{
			m_httpClient = HttpClients.custom()
					.setRedirectStrategy(new LaxRedirectStrategy())
					.build();
			m_requestConfig = RequestConfig.custom()
					.setSocketTimeout(m_waitTime)
					.setConnectTimeout(m_waitTime)
					.setConnectionRequestTimeout(m_waitTime)
					.build();
		}
		m_uDetector = new UniversalDetector(null);
		HttpURLConnection.setFollowRedirects(true);
//		m_dfsConfig = new Configuration();
		m_accessUser = "spark";
		//m_dfsConfig.set("fs.default.name", "hdfs://172.25.198.11:8020/");
//		m_dfsConfig.addResource(new Path("/usr/local/hadoop-2.5.0-cdh5.3.9/etc/hadoop/core-site.xml"));
//		m_dfsConfig.addResource(new Path("/usr/local/hadoop-2.5.0-cdh5.3.9/etc/hadoop/hdfs-site.xml"));
		
		m_hdfsPath = "/user/" + m_accessUser + "/html/";
		//UserGroupInformation ugi = UserGroupInformation.createRemoteUser("hdfs");
		//m_dfsConfig.set("hadoop.job.ugi", "hdfs");
		System.setProperty("HADOOP_USER_NAME", "hdfs");
		
		m_hbaseConfig = HBaseConfiguration.create();
		m_hbaseConfig.addResource(new Path("/usr/local/hadoop-2.5.0-cdh5.3.9/etc/hadoop/core-site.xml"));
		m_hbaseConfig.addResource(new Path("/usr/local/hadoop-2.5.0-cdh5.3.9/etc/hadoop/hdfs-site.xml"));
		m_hbaseConfig.addResource(new Path("/usr/local/hadoop-2.5.0-cdh5.3.9/etc/hadoop/hbase-site.xml"));
		m_hbaseConfig.set("hbase.client.write.buffer","134217728");
		m_hbaseConfig.set("hbase.client.keyvalue.maxsize","0");
		
		//System.out.print(m_dfsConfig.get("fs.defaultFS"));
		//System.out.print(m_dfsConfig.get("hadoop.job.ugi"));
//		try {
//			m_dfs = FileSystem.get(FileSystem.getDefaultUri(m_dfsConfig),m_dfsConfig,m_accessUser);
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
	}
	
//	protected void finalize(){
//		try {
//			m_dfs.close();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}
	
	public String GetOsUserName(){
		return m_osUserName;
	}
	
 	public String getHtml(String url){	
		String html=null, encoding;
		byte[] data=null;
		
		try{
			HttpGet httpGet = new HttpGet(url);
			httpGet.setConfig(m_requestConfig);
			CloseableHttpResponse response = m_httpClient.execute(httpGet);
			HttpEntity entity = response.getEntity();
			//System.out.print("Entity, ");
			if(entity != null){
				InputStream in = entity.getContent();
				ByteArrayOutputStream bao = new ByteArrayOutputStream();
				byte[] buff = new byte[4096];
				int bytesRead;

				while((bytesRead = in.read(buff)) > 0 ){
					if(!m_uDetector.isDone())
						m_uDetector.handleData(buff, 0, bytesRead);
					bao.write(buff, 0, bytesRead);
				}
				m_uDetector.dataEnd();
				data = bao.toByteArray();
				encoding = m_uDetector.getDetectedCharset();
				if(encoding != null){
					html = new String(data,encoding);
					if(encoding != "UTF-8")
						html = new String(html.getBytes("UTF-8"), "UTF-8");
				}	
				else{
					html = new String(data, "UTF-8");
				}
			}
			//System.out.print(html);
			Document doc = Jsoup.parse(html, "", Parser.xmlParser().setTrackErrors(0));
			//check encoding again
			String detected_ec = doc.charset().name();
			String page_cs_str = doc.select("meta[http-equiv=\"Content-Type\"]").attr("content");
			if(!page_cs_str.equals("")){
				String[] outter = page_cs_str.split(";");
				if(outter.length > 1){
					String[] inner = outter[1].split("=");
					if(inner.length > 1){
						detected_ec = inner[1].trim();
					}
				}
			}
			else{
				String page_cs_str_01 = doc.select("meta").attr("charset");
				if(!page_cs_str_01.equals("")){
					detected_ec = page_cs_str_01.trim();
				}
			}
			//if not coincident with predict charset
			if(!detected_ec.equals(doc.charset().name())){
				html = new String(data, detected_ec);
			}
			
			response.close();
		}
		catch(Exception e){
			//System.out.println(e.toString());
		}
		finally{
			m_uDetector.reset();
		}
		if(html != null)
			html = html.trim();
		return html;
	}
	
	public boolean writeToLocal(String contents, String name, boolean append){
		BufferedWriter bw;
		try {
			File f = new File("/home/"+m_osUserName+"/Data/output/Error/"+name+".txt");
			if(f.exists())
				bw= new BufferedWriter(new OutputStreamWriter(new FileOutputStream(f, append)));
			else{
				if(!f.getParentFile().exists())
					f.getParentFile().mkdirs();
				bw= new BufferedWriter(new OutputStreamWriter(new FileOutputStream(f, false)));
			}
				
			bw.write(contents);
			bw.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return true;
	}
	
//	public boolean writeToHDFS(String contents, String name){
//		return writeToHDFS(contents, name, false);
//	}
	
//	public boolean writeToHDFS(String contents, String name, boolean append){	
//		FSDataOutputStream os;
//		Path p = new Path(m_hdfsPath + name + ".txt");
//    	try {
//    		if(!append){
////	    		if(m_dfs.exists(p)){
////	    			return true;
////	    			//m_dfs.delete(p, false);
////	    		}
//				os = m_dfs.create(p);
//    		}
//    		else{
//    			if(!m_dfs.exists(p)){
//    				os = m_dfs.create(p);
//    			}
//    			else{
//    				os = m_dfs.append(p);
//    			}
//    		}
//    		os.write(contents.getBytes("UTF-8"));
//			os.close();
//    			
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//			
//			//os.close();
//			return false;
//		}
//		return true;
//	}
	
//	public String readFromHdfs(String name){
//		Path p = new Path(m_hdfsPath + name + ".txt");
//		String html="", tmp;
//		try{
//			FSDataInputStream in = m_dfs.open(p);
//			BufferedReader br = new BufferedReader(new InputStreamReader(in, "UTF-8"));
//			while((tmp=br.readLine()) != null){
//				html += (tmp + "\n");
//			}
//			br.close();
//			in.close();
//		}
//		catch(Exception e){
//			e.printStackTrace();
//		}
//		//System.out.print(html);
//		return html.trim();
//	}
	
//	public boolean writeSequenceFile(String key, String value, String name){
//		LongWritable key_ = new LongWritable();
//		Text val_ = new Text();
//		Path path = new Path(m_hdfsPath + name + ".dat");
//		SequenceFile.Writer writer = null;
//		try{
//			writer = SequenceFile.createWriter(m_dfsConfig,
//					Writer.file(path),
//					Writer.keyClass(key_.getClass()),
//					Writer.valueClass(val_.getClass()),
//					Writer.compression(SequenceFile.CompressionType.BLOCK, new DefaultCodec()),
//					Writer.metadata(new Metadata()));
//			
//			val_.set("1	test");
//			key_.set(Long.parseLong(key));
//			writer.append(key_, val_);
//			writer.close();
//		}
//		catch(Exception e){
//			e.printStackTrace();
//		}
//		
//		
//		return true;
//	}
	
//	public boolean writeToHBase(BufferedMutator table, String row_key, String family, String qualifier, String value) throws IOException{
//		Put p = new Put(Bytes.toBytes(row_key));
//		if(qualifier == null)
//			p.addColumn(Bytes.toBytes(family), null, Bytes.toBytes(value));
//		else
//			p.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
//		table.mutate(p);
//		
//		return true;
//	}
	
//    public static void main( String[] args )
//    {
//    	if(args.length<4){
//    		System.out.println("the number of input arguments is less than 3!");
//    		return;
//    	}
//    	
//    	HtmlCrawler crawler = null;
//    	BufferedReader br = null;
//    	String line_str, err_msg;
//    	String[] tokens;
//    	Connection conn=null;
//    	Table table = null;
//    	BufferedMutator mutator=null;
//    	long skip_count=0, insert_count=0, error_count=0;
//    	long skip_time=0, insert_time=0, error_time=0;
//    	List<Get> gets = new ArrayList<Get>();
//    	List<String[]> token_list = new ArrayList<String[]>();
//    	int mini_batch_size = Integer.parseInt(args[2]);
//    	long time, hbase_get_time;
//    	
//    	if(args.length >=5)
//    		if(args[3].toLowerCase().equals("true"))
//    			crawler = new HtmlCrawler(args[4], true);
//    		else
//    			crawler = new HtmlCrawler(args[4], false);
//    	else
//    		crawler = new HtmlCrawler(null, false);
//    	//crawler.writeToLocal("test","abc",true);
//    	try{
//    		//HTable table = new HTable(crawler.m_hbaseConfig, "test");
//    		conn = ConnectionFactory.createConnection(crawler.m_hbaseConfig);
//    		//System.out.println(crawler.m_hbaseConfig.get("hbase.client.write.buffer"));
//    		table = conn.getTable(TableName.valueOf("url_info"));
//    		mutator = conn.getBufferedMutator(TableName.valueOf("url_info"));
////        	Get g = new Get(Bytes.toBytes("1"));
////        	Result r = table.get(g);
////        	Put p = new Put(Bytes.toBytes("1"));
////        	p.addColumn(Bytes.toBytes("c1"), Bytes.toBytes("t1"), Bytes.toBytes("6"));
////        	p.addColumn(Bytes.toBytes("c2"), Bytes.toBytes("m1"), Bytes.toBytes("Haoyang"));
////        	table.put(p);
//    		br = new BufferedReader(new InputStreamReader(new FileInputStream("/home/"+crawler.GetOsUserName()+"/Data/input/"+args[0])));
//    		line_str = br.readLine();
//    		//long time = System.currentTimeMillis();
//    		while(line_str != null && !"".equals(line_str)){
//    			tokens = line_str.split(",");
//    			if(tokens.length != 2){
//    				err_msg = "Invalid input url: " + line_str + "\n";
//    				System.out.print(err_msg);
//    				continue;
//    			}
//    			//Path tmp_path = new Path(crawler.m_hdfsPath + tokens[0] + ".txt");
//    			token_list.add(tokens);
//				Get g = new Get(Bytes.toBytes(tokens[0]));
//				//Result r = table.get(g);
//				gets.add(g);
//				
//				line_str = br.readLine();
//				
//				if(line_str == null || "".equals(line_str) || gets.size() == mini_batch_size){
//					int batch_size = gets.size();
//					time = System.currentTimeMillis();
//					Result[] rs = table.get(gets);
//					hbase_get_time = (System.currentTimeMillis()-time) / batch_size;
//					for(int i=0; i<batch_size; i++){
//						System.out.print(token_list.get(i)[0] + ": ");
//						time = System.currentTimeMillis();
//						if(rs[i].isEmpty()){
//			    			String html = crawler.getHtml(token_list.get(i)[1]);
//			    			if(html != null && !"".equals(html)){
//			    				crawler.writeToHBase(mutator, token_list.get(i)[0], "raw_html", null, html);
//			    				System.out.print("INSERT\n");
//			    				insert_count++;
//			    				insert_time += (System.currentTimeMillis() - time + hbase_get_time);
//			    			}else{
//			    				err_msg = line_str + "\n"; 
//			    				crawler.writeToLocal(err_msg, "error_" + args[1], true);
//			    				System.out.print("Local\n");
//			    				error_count++;
//			    				error_time += (System.currentTimeMillis() - time + hbase_get_time);
//			    			}
//		    				//crawler.writeSequenceFile(tokens[0], crawler.readFromHdfs(tokens[0]), "seq_"+args[1]);
//		    				//crawler.writeToHBase(mutator, tokens[0], "raw_html", null, crawler.readFromHdfs(tokens[0]));
//		    				//mutator.flush();
//						}else{
//							System.out.print("SKIP\n");
//							skip_count++;
//							skip_time += (System.currentTimeMillis() - time + hbase_get_time);
//						}
//					}
//					token_list.clear();
//					gets.clear();
//				}
//				
//    			//if(crawler.m_dfs.exists(tmp_path)){
//				
//				//crawler.m_dfs.delete(tmp_path, false);
//				//line_str = br.readLine();
//				//continue;
//    			//}
//    			
//    		}
//   			//System.out.println("Run Time: " + (System.currentTimeMillis()-time) + "ms");
//   			System.out.println("INSERTED: " + insert_count);
//   			System.out.println("INSERT_TIME: " + insert_time);
//   			System.out.println("SKIPPED: " + skip_count);
//   			System.out.println("SKIP_TIME: " + skip_time);
//   			System.out.println("ERROR: " + error_count);
//   			System.out.println("ERROR_TIME: " + error_time);
//    		br.close();
//    		table.close();
//    		mutator.close();
//    		conn.close();
//    		
//    		
//    	}
//    	catch(Exception e){
//    		e.printStackTrace();
//    	}   	
//    }
}
