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
	
	public static void main(String[] args) {
		ConfigurableApplicationContext ctx = new GenericXmlApplicationContext();
        ConfigurableEnvironment env = ctx.getEnvironment();
        MutablePropertySources propertySources = env.getPropertySources();
        
        try {
            propertySources.addLast(new ResourcePropertySource("classpath:db.properties"));
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

		Executor hd = new Executor();
		hd.readHadoop(env);
	}
	
	public void readHadoop(ConfigurableEnvironment env) {
		
		Connection conn = null;
        ResultSet rs = null;
		try{
	        System.out.println("######### Executor 실행 #########");
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
	        
//	        String sql = "SELECT  a.m330_cret_id as m330_cret_id, a.m310_arti_id as  m310_arti_id, SUBSTR(a.m310_arti_kor_titl,0,20) AS m310_arti_kor_titl " +
//	        		 	 ", d.m330_belo_insi_id as m330_belo_insi_id , SUBSTR(d.m330_belo_insi_nm,0,20) AS m330_belo_insi_nm, D.M330_KRI_ID as m330_kri_id " +
//	        		 	 ",D.M330_CRET_KOR_NM as m330_cret_kor_nm " + 
//	        		 	 "FROM (SELECT C.M330_CRET_ID,B.M310_ARTI_ID,B.M310_ARTI_KOR_TITL, C.D311_BELO_INSI_ID, C.D311_BELO_INSI_NM " + 
//	        		 	 "FROM   ODS_KCI.KCDM310 B, ODS_KCI.KCDD311 C " + 
//	        		 	 "WHERE  B.M310_ARTI_ID= C.M310_ARTI_ID) A, ODS_KCI.KCDM330 D " + 
//	        		 	 "WHERE D.M330_CRET_ID=a.m330_cret_id limit 50";
	        String sql = "SELECT  a.m330_cret_id as m330_cret_id, a.m310_arti_id as m310_arti_id,\r\n" + 
	        		" SUBSTR(a.m310_arti_kor_titl,0,20) AS m310_arti_kor_titl, d.m330_belo_insi_id as m330_belo_insi_id, SUBSTR(d.m330_belo_insi_nm,0,20) AS m330_belo_insi_nm, \r\n" + 
	        		"d.m330_kri_id as m330_kri_id, d.m330_cret_kor_nm as m330_cret_kor_nm\r\n" + 
	        		"FROM (SELECT C.M330_CRET_ID,B.M310_ARTI_ID,B.M310_ARTI_KOR_TITL, C.D311_BELO_INSI_ID, C.D311_BELO_INSI_NM\r\n" + 
	        		"FROM   ods_kci.KCDM310 B, ods_kci.KCDD311 C\r\n" + 
	        		"WHERE  B.M310_ARTI_ID= C.M310_ARTI_ID) A, ods_kci.KCDM330 D\r\n" + 
	        		"WHERE D.M330_CRET_ID=a.m330_cret_id limit 10000";
	        rs = stmt.executeQuery(sql);
	        ResultSetMetaData rsmd = rs.getMetaData();
	        List<String> columns = new ArrayList<String>(rsmd.getColumnCount());
	        for(int i = 1; i <= rsmd.getColumnCount(); i++){
	            columns.add(rsmd.getColumnName(i));
	        }
	        
	        List<Map<String,String>> data = new ArrayList<Map<String,String>>();
	        int i=0;
	        rs.next();
	        while(true){
	        	 i++;
	        	 HashMap<String,String> row = new HashMap<String, String>(columns.size());
	             for(String col : columns) {
	                 row.put(col, rs.getString(col));
	             }
	             data.add(row);
	        
	             if(i == 1000) {
	            	 System.out.println("CYPHER START");
	            	 createNeo4jExecutor(neo4j_url, neo4j_username, neo4j_password, data);
	            	 data = new ArrayList<Map<String,String>>();
	            	 System.out.println("CYPHER END");
	            	 i=0;
	             }
	             if(!rs.next()) {
	            	 System.out.println("LAST CYPHER START");
	            	 createNeo4jExecutor(neo4j_url, neo4j_username, neo4j_password, data);
	            	 System.out.println("LAST CYPHER END");
	            	 break;
	             }
	        
	        }
	        
	        rs.close();
	        conn.close();
	        System.out.println("######### Executor 종료 #########");
	        
	    }catch(Exception ex){
	        System.err.println("main error :");
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

	public void createNeo4jExecutor(String uri, String username, String password, List<Map<String,String>> data) {

		 try ( CreateNeo4j greeter = new CreateNeo4j( uri, username, password) )
         {
             greeter.addData(data);
             greeter.close();
         } catch (Exception e) {
 			// TODO Auto-generated catch block
 			e.printStackTrace();
 		 }
	}

}
