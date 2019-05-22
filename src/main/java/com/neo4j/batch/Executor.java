package com.neo4j.batch;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.io.support.ResourcePropertySource;

/**
 * 2019-04-26
 * Hadoop에서 데이터를 READ한 후 Neo4j에 테이블을 만들어주는 메소드를 실행하는 클래스
 * @author song
 *
 */
public class Executor {
	
	private static final Logger logger = LoggerFactory.getLogger(Executor.class);
	
	public static void main(String[] args) {
		ConfigurableApplicationContext ctx = new GenericXmlApplicationContext();
        final ConfigurableEnvironment env = ctx.getEnvironment();
        MutablePropertySources propertySources = env.getPropertySources();
        
        try {
            propertySources.addLast(new ResourcePropertySource("classpath:db.properties"));
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        long startTime = System.currentTimeMillis();

        /*    */
        // 논문
//        Thread t1 = new Thread(new Runnable() {
//        	@Override
//        	public void run() {
//				Executor hd = new Executor();
//				hd.readHadoop(env, 1);
//			}
//		});
//        t1.start();
        
        // 연구자
        Thread t2 = new Thread(new Runnable() {
        	@Override
        	public void run() {
        		Executor hd = new Executor();
        		hd.readHadoop(env, 2);
        	}
        });
        t2.start();
        
        // 기관
//        Thread t3 = new Thread(new Runnable() {
//        	@Override
//        	public void run() {
//        		Executor hd = new Executor();
//        		hd.readHadoop(env, 3);
//        	}
//        });
//        t3.start();
        
        // Pub
//        Thread t4 = new Thread(new Runnable() {
//        	@Override
//        	public void run() {
//        		Executor hd = new Executor();
//        		hd.readHadoop(env, 4);
//        	}
//        });
//        t4.start();
        
        /**/
        // 위 노드 만들기가 끝날때까지 기다린다
        try {
//        	t4.join();
//        	t3.join();
        	t2.join();
//			t1.join();
			System.out.println("CreateNeo4j 전체 Thread 종료");
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
        /*
        new Thread(new Runnable() {
        	@Override
        	public void run() {
				Executor hd = new Executor();
				hd.createRelation(env, 1);
			}
		}).start();
        new Thread(new Runnable() {
        	@Override
        	public void run() {
        		Executor hd = new Executor();
        		hd.createRelation(env, 2);
        	}
        }).start();
        new Thread(new Runnable() {
        	@Override
        	public void run() {
        		Executor hd = new Executor();
        		hd.createRelation(env, 3);
        	}
        }).start();
        */
       
        long endTime = System.currentTimeMillis();
        long time = endTime - startTime;
        System.out.println("실행시간 =============== " + time/1000.0 + "초");
        
	}
	
	public void readHadoop(ConfigurableEnvironment env, int seq) {
		
		Connection conn = null;
        ResultSet rs = null;
		try{
	        System.out.println("######### " + seq + "번째  Executor 실행 #########");
	        String driver = env.getProperty("hive.driver");
	        Class.forName(driver);
	        
	        String hive_url = env.getProperty("hive.url");
	        String hive_username = env.getProperty("hive.username");
	        String hive_pw = env.getProperty("hive.password");
	        String neo4j_url = env.getProperty("neo4j.url");
	        String neo4j_username = env.getProperty("neo4j.username");
	        String neo4j_password = env.getProperty("neo4j.password");
	        
	        conn = DriverManager.getConnection(hive_url, hive_username, hive_pw);
	        
	        Statement stmt = conn.createStatement();
	        
	        String sql = "";
	        if(seq == 1) { // 논문
//	        	sql ="SELECT  \r\n" + 
//	        			"B.m310_arti_kor_titl as m310_arti_kor_titl, B.m310_arti_id as m310_arti_id\r\n" + 
//	        			", A.d311_belo_insi_nm as d311_belo_insi_nm\r\n" + 
//	        			"FROM  ods_kci.kcdd311 A, ods_kci.kcdm310 B\r\n" + 
//	        			"WHERE A.m310_arti_id = B.m310_arti_id\r\n" + 
//	        			"and A.d311_belo_insi_nm is not null and trim(A.d311_belo_insi_nm) != ''";
	        	sql = "SELECT  \r\n" + 
	        			"B.m310_arti_kor_titl as m310_arti_kor_titl, B.m310_arti_id as m310_arti_id\r\n" + 
	        			", rtrim(ltrim(trim(A.d311_belo_insi_nm))) as d311_belo_insi_nm\r\n" + 
	        			"FROM ods_kci.kcdd311 A, ods_kci.kcdm310 B\r\n" + 
	        			"WHERE A.m310_arti_id = B.m310_arti_id\r\n" + 
//	        			"and rtrim(ltrim(trim(A.d311_belo_insi_nm))) in ('한국원자력안전기술원', '한국은행', '한국인터넷진흥원')\r\n" +
	        			"and A.d311_belo_insi_nm is not null and trim(A.d311_belo_insi_nm) != ''";
	        }else if(seq == 2){ // 연구자
//	        	sql = "SELECT  \r\n" + 
//	        			"  B.m330_cret_kor_nm as m330_cret_kor_nm\r\n" + 
//	        			", A.m310_arti_id as m310_arti_id\r\n" + 
//	        			", B.m330_belo_insi_nm as m330_belo_insi_nm\r\n" + 
//	        			"FROM  ods_kci.kcdd311 A, ods_kci.kcdm330 B\r\n" + 
//	        			"WHERE A.m330_cret_id = B.m330_cret_id\r\n" + 
//	        			"and B.m330_belo_insi_nm is not null and trim(B.m330_belo_insi_nm) != ''";
//	        	sql = "SELECT  \r\n" + 
//	        			"  B.m330_cret_kor_nm as m330_cret_kor_nm\r\n" + 
//	        			"  B.m330_cret_id as m330_cret_id\r\n" + 
//	        			", A.m310_arti_id as m310_arti_id\r\n" + 
//	        			", rtrim(ltrim(trim(B.m330_belo_insi_nm))) as m330_belo_insi_nm\r\n" + 
//	        			"FROM ods_kci.kcdd311 A, ods_kci.kcdm330 B\r\n" + 
//	        			"WHERE A.m330_cret_id = B.m330_cret_id\r\n" + 
////	        			"and rtrim(ltrim(trim(B.m330_belo_insi_nm))) in ('한국원자력안전기술원', '한국은행', '한국인터넷진흥원')\r\n" +
//	        			"and B.m330_belo_insi_nm is not null and trim(B.m330_belo_insi_nm) != ''";
	        	sql = "SELECT  \r\n" + 
	        			"  B.m330_cret_kor_nm as m330_cret_kor_nm\r\n" + 
	        			"  ,B.m330_cret_id as m330_cret_id\r\n" + 
	        			"FROM ods_kci.kcdm330 B where m330_cret_kor_nm is not null"; 
	        }else if(seq == 3) { // 기관
//	        	sql = "select distinct m330_belo_insi_nm as m330_belo_insi_nm from ods_kci.KCDM330 \r\n" + 
//	        			"where m330_belo_insi_nm is not null and trim(m330_belo_insi_nm) != ''";
	        	sql = "select distinct rtrim(ltrim(trim(m330_belo_insi_nm))) as m330_belo_insi_nm from ods_kci.KCDM330 \r\n" + 
	        			"where m330_belo_insi_nm is not null and trim(m330_belo_insi_nm) != ''";
//	        			"and rtrim(ltrim(trim(m330_belo_insi_nm))) in ('한국원자력안전기술원', '한국은행', '한국인터넷진흥원')";
	        }else if(seq == 4){ // 공저자
	        	sql = "select\r\n" + 
	        			"A.m310_arti_id as m310_arti_id, A.m330_cret_id as m330_cret_id, B.m310_arti_kor_titl as m310_arti_kor_titl\r\n" + 
	        			"from\r\n" + 
	        			"(\r\n" + 
	        			"select m310_arti_id, m330_cret_id\r\n" + 
	        			"from ods_kci.kcdd311\r\n" + 
	        			"where m310_arti_id is not null and m330_cret_id is not null\r\n" + 
	        			") A, ods_kci.kcdm310 B\r\n" + 
	        			"where A.m310_arti_id = B.m310_arti_id and B.m310_arti_kor_titl is not null";
	        }
	        
	        rs = stmt.executeQuery(sql);
	        ResultSetMetaData rsmd = rs.getMetaData();
	        List<String> columns = new ArrayList<String>(rsmd.getColumnCount());
	        for(int i = 1; i <= rsmd.getColumnCount(); i++){
	            columns.add(rsmd.getColumnName(i));
	        }
	        System.out.println("#########" + seq + "번째 Executor 쿼리 실행완료 #########");
	        System.out.println("#########" + seq + "번째 Neo4j Cypher 시작 ##########");
	        List<Map<String,String>> data = new ArrayList<Map<String,String>>();
	        int i=0;
	        rs.next();
	        while(true){
	        	 try {
					i++;
					 HashMap<String,String> row = new HashMap<String, String>(columns.size());
					 for(String col : columns) {
					     row.put(col, rs.getString(col));
					 }
					 data.add(row);
					 if(!rs.next()) {
						 createNeo4jExecutor(neo4j_url, neo4j_username, neo4j_password, data, seq);
						 break;
					 }
      
					 if(i == 1000) {
						 createNeo4jExecutor(neo4j_url, neo4j_username, neo4j_password, data, seq);
						 data = new ArrayList<Map<String,String>>();
						 i=0;
					 }
				} catch (Exception e) {
					logger.error(e.getMessage());
					System.out.println("#########" + seq + "번째 Neo4j Cypher 실패 ##########");
					break;
				}
	        
	        }
	        
	        rs.close();
	        conn.close();
	        System.out.println("#########" + seq + "번째 Neo4j Cypher 종료 ##########");
	        
	    }catch(Exception ex){
	        ex.printStackTrace();
	    }finally {
	        try{
	            if( rs != null ){
	                rs.close();                
	            }
	        }catch(Exception ex){
	            rs = null;
	        }
	        
	        try{
	            if( conn != null ){
	                conn.close();                
	            }
	        }catch(Exception ex){
	            conn = null;
	        }
	    }
	}
	
	public void createRelation(ConfigurableEnvironment env, int seq) {
		
		try{
			System.out.println("######### " + seq + "번째 Relation  Executor 실행 #########");
			String driver = env.getProperty("hive.driver");
			Class.forName(driver);
			
			String neo4j_url = env.getProperty("neo4j.url");
			String neo4j_username = env.getProperty("neo4j.username");
			String neo4j_password = env.getProperty("neo4j.password");
			
			createRelationExecutor(neo4j_url, neo4j_username, neo4j_password, seq);
			
			System.out.println("#########" + seq + "번째 Relation Cypher 종료 ##########");
			
		}catch(Exception ex){
			ex.printStackTrace();
		}
	}

	public void createNeo4jExecutor(String uri, String username, String password, List<Map<String,String>> data, int seq) throws Exception {

		 try ( CreateNeo4j greeter = new CreateNeo4j( uri, username, password) )
         {
             greeter.addData(data, seq);
             greeter.close();
         } catch (Exception e) {
 			throw new Exception(e);
 		 }
	}
	
	public void createRelationExecutor(String uri, String username, String password, int seq) throws Exception {
		
		try ( CreateRelation greeter = new CreateRelation( uri, username, password) )
		{
			greeter.addRelation(seq);
			greeter.close();
		} catch (Exception e) {
			throw new Exception(e);
		}
	}
	
	public void createUniqueExecutor(String uri, String username, String password, int seq) throws Exception {
		
		try ( CreateUnique greeter = new CreateUnique( uri, username, password) )
		{
			greeter.addUnique(seq);
			greeter.close();
		} catch (Exception e) {
			throw new Exception(e);
		}
	}

}
