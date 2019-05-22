package com.neo4j.batch;

import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Transaction;

public class CreateRelation implements AutoCloseable {
	
	private final Driver driver;

	public CreateRelation( String uri, String user, String password )
	{
		Config noSSL = Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig(); // 서버가 ssl을 사용하지 않는다면 해당 구문 필요
	    driver = GraphDatabase.driver( uri, AuthTokens.basic( user, password ), noSSL );
	}

    @Override
    public void close() throws Exception
    {
        driver.close();
    }

    public void addRelation(int seq)
    {
        try (Session session = driver.session())
        {
            try (Transaction tx = session.beginTransaction())
            {
            	if(seq == 1) {
            		tx.run(
            				"call apoc.periodic.iterate(\"\r\n" + 
            				"match (n:Thesis), (r:Researcher)\r\n" + 
            				"where n.thesis_id = r.thesis_id\r\n" + 
            				"return r,n\",\"\r\n" + 
            				"create (n)-[y:P_Acheive]->(r)\r\n" + 
            				"create(r)-[z:Author]->(n)\r\n" + 
            				"\",{batchSize:10000, parallel:false})"
    				);
            	}else if(seq == 2) {
            		tx.run(
            				"call apoc.periodic.iterate(\"\r\n" + 
            				"match (n:Thesis), (r:Organ)\r\n" + 
            				"where n.organ_nm = r.name\r\n" + 
            				"return r,n\",\"\r\n" + 
            				"create (n)-[y:O_Acheive]->(r)\r\n" + 
            				"create (r)-[z:Affiliation]->(n)\r\n" + 
            				"\",{batchSize:10000, parallel:false})"
            				);
            	}else if(seq == 3) {
            		tx.run(
            				"call apoc.periodic.iterate(\"\r\n" + 
            				"match (n:Organ), (r:Researcher)\r\n" + 
            				"where n.name = r.organ_nm\r\n" + 
            				"return r,n\",\"\r\n" + 
            				"create (n)-[y:Be_Workded]->(r)\r\n" + 
            				"create (r)-[z:Hired]->(n)\r\n" + 
            				"\",{batchSize:10000, parallel:false})"
            				);
            	}
                tx.success();
            }
        }
    }
}
