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
		Config noSSL = Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig(); // 서버가 ssl을 사용하지 않는다면 해당 구문 필요
	    driver = GraphDatabase.driver( uri, AuthTokens.basic( user, password ), noSSL );
	}

    @Override
    public void close() throws Exception
    {
        driver.close();
    }

    public void addData(List<Map<String,String>> list, int seq)
    {
        try (Session session = driver.session())
        {
            try (Transaction tx = session.beginTransaction())
            {
            	for (Map<String, String> hm : list) {
            		if(seq == 1) { // 논문
            			tx.run("CREATE (p:Thesis {name: $a, thesis_id: $b, organ_nm: $c})"
            					, parameters
            					(
        							"a", hm.get("m310_arti_kor_titl"),
        							"b", hm.get("m310_arti_id"),
        							"c", hm.get("d311_belo_insi_nm")
    							)
    					);
            		}else if(seq == 2) { // 연구자
            			tx.run("CREATE (r:Author {name: $a, cret_id: $b})"
            					, parameters
            					(
        							"a", hm.get("m330_cret_kor_nm"),
        							"b", hm.get("m330_cret_id")
    							)
    					);
//            			tx.run("CREATE (r:Author {name: $a, cret_id: $b, thesis_id: $c, organ_nm: $d})"
//            					, parameters
//            					(
//        							"a", hm.get("m330_cret_kor_nm"),
//        							"b", hm.get("m330_cret_id"),
//        							"c", hm.get("m310_arti_id"),
//        							"d", hm.get("m330_belo_insi_nm")
//    							)
//    					);
            		}else if(seq == 3) { // 기관
            			tx.run("CREATE (q:Organ {name: $a})"
            					, parameters
            					(
        							"a", hm.get("m330_belo_insi_nm")
    							)
    					);
            		}else if(seq == 4) { // Pub
            			tx.run("CREATE (q:Pub2 {c_id: $a, thesis_id: $b, thesis_nm: $c})"
            					, parameters
            					(
        							"a", hm.get("m330_cret_id"),
        							"b", hm.get("m310_arti_id"),
        							"c", hm.get("m310_arti_kor_titl")
    							)
    					);
            		}
            	}
                tx.success();
            }
        }
    }
}
