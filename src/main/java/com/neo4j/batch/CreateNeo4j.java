package com.neo4j.batch;

import static org.neo4j.driver.v1.Values.parameters;

import java.util.List;
import java.util.Map;

import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Transaction;


/**
 * 2019-04-26
 * Hadoop에서 가져온 데이터를 Neo4j cyper로 변경하여 테이블을 만들어주는 클래스
 * @author song
 *
 */
public class CreateNeo4j implements AutoCloseable {
	 private final Driver driver;

	    public CreateNeo4j( String uri, String user, String password )
	    {
	    	Config noSSL = Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig(); 
	        driver = GraphDatabase.driver( uri, AuthTokens.basic( user, password ), noSSL );
	    }

	    @Override
	    public void close() throws Exception
	    {
	        driver.close();
	    }

	    public void addData(List<Map<String,String>> list)
	    {
	        // Sessions are lightweight and disposable connection wrappers.
	        try (Session session = driver.session())
	        {
	            // Wrapping Cypher in an explicit transaction provides atomicity
	            // and makes handling errors much easier.
	            try (Transaction tx = session.beginTransaction())
	            {
	            	for (Map<String, String> hm : list) {
	            		tx.run("CREATE (p:Thesis {Title: $a, author_nm: $b, author_id: $c, thesis_id: $d, Organ_id: $e})"
		            			, parameters
		            			(
		            					"a", hm.get("m310_arti_kor_titl"),
		            					"b", hm.get("m330_cret_kor_nm"),
		            					"c", hm.get("m330_kri_id"),
		            					"d", hm.get("m310_arti_id"),
		            					"e", hm.get("d311_belo_insi_id")
		            			)
            			);
		                tx.run("CREATE (q:Organ {name: $a ,Org_id: $b})"
		                		, parameters
		            			(
		            					"a", hm.get("m330_belo_insi_nm"),
		            					"b", hm.get("m330_belo_insi_id")
		            			)
		            	);
		                tx.run("CREATE (r:Researcher {name: $a, r_id: $b, thesis_id: $c, organ_id: $d})"
		                		, parameters
		            			(
		            					"a", hm.get("m330_cret_kor_nm"),
		            					"b", hm.get("m330_kri_id"),
		            					"c", hm.get("m310_arti_id"),
		            					"d", hm.get("m330_belo_insi_id")
		            			)
            			);
	            		/*
		            	tx.run("CREATE (p:논문 {id: $a}) set p.Author = $b, p.title = $c"
		            			+ ", p.Organ_id= $d, p.author= $e, p.author_id= $f"
		            			, parameters
		            			(
		            					"a", hm.get("m310_arti_kor_titl"),
		            					"b", hm.get("m330_kri_id"),
		            					"c", hm.get("m310_arti_id"),
		            					"d", hm.get("d311_belo_insi_id"),
		            					"e", hm.get("m330_cret_kor_nm"),
		            					"f", hm.get("m330_kri_id")
		            			)
            			);
		                tx.run("CREATE (q:기관 {id: $a}) "
		                		+ "set q.Org_name= $b"
		                		, parameters
		            			(
		            					"a", hm.get("d311_belo_insi_nm"),
		            					"b", hm.get("d311_belo_insi_id")
		            			)
		            	);
		                tx.run("CREATE (r:저자 {au_id: $a}) "
		                		+ "set r.au_name = $b, r.thesis_id = $c, r.organ_id = $d"
		                		, parameters
		            			(
		            					"a", hm.get("m330_cret_kor_nm"),
		            					"b", hm.get("m330_kri_id"),
		            					"c", hm.get("m310_arti_id"),
		            					"d", hm.get("d311_belo_insi_id")
		            			)
            			);
            			*/
            			
	            	}
	                tx.success();  // Mark this write as successful.
	            }
	        }
	    }
}
