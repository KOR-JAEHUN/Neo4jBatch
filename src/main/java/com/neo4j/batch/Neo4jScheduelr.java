package com.neo4j.batch;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.GenericXmlApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.io.support.ResourcePropertySource;
import org.springframework.scheduling.annotation.Scheduled;

public class Neo4jScheduelr  {
	
    private static final Logger logger = LoggerFactory.getLogger(Neo4jScheduelr.class);
   
//    @Scheduled(cron="* 10 * * * *") 
    @Scheduled(fixedDelay=1000*10) 
    public void scheduleRun() {
 
    	ConfigurableApplicationContext ctx = new GenericXmlApplicationContext();
        final ConfigurableEnvironment env = ctx.getEnvironment();
        MutablePropertySources propertySources = env.getPropertySources();
        
        try {
            propertySources.addLast(new ResourcePropertySource("classpath:db.properties"));
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    	
    	Calendar calendar = Calendar.getInstance();
 
    	SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
 
    	logger.info("스케줄 실행 : " + dateFormat.format(calendar.getTime()));
    	
    	 // 논문
        new Thread(new Runnable() {
        	@Override
        	public void run() {
				Executor hd = new Executor();
				hd.readHadoop(env, 1);
			}
		}).start();
        
        // 연구자
        new Thread(new Runnable() {
        	@Override
        	public void run() {
        		Executor hd = new Executor();
        		hd.readHadoop(env, 2);
        	}
        }).start();
        
        // 기관
        new Thread(new Runnable() {
        	@Override
        	public void run() {
        		Executor hd = new Executor();
        		hd.readHadoop(env, 3);
        	}
        }).start();;
    }
    
}
