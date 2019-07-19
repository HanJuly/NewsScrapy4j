package com.han.scrapy.dao;

import com.han.scrapy.pojo.News;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.beans.PropertyVetoException;
import java.util.ArrayList;
import java.util.List;

public class NewsDao extends JdbcTemplate {
    private static ComboPooledDataSource c3P0PooledDataSource;
    private final static Logger LOGGER = LoggerFactory.getLogger(NewsDao.class);

    public NewsDao() {
        if (c3P0PooledDataSource == null) {
            c3P0PooledDataSource = new ComboPooledDataSource();
            try {
                c3P0PooledDataSource.setDriverClass("com.mysql.jdbc.Driver");
                c3P0PooledDataSource.setUser("root");
                c3P0PooledDataSource.setPassword("123456");
                c3P0PooledDataSource.setJdbcUrl("jdbc:mysql://192.168.72.141:3306/gossip?useSSL=false&characterEncoding=utf8&serverTimezone=UTC");
            } catch (PropertyVetoException e) {
                e.printStackTrace();

            }
        }
        this.setDataSource(c3P0PooledDataSource);
    }

    public void saveData(List<News> news) {
        List<Object[]> placeholder = new ArrayList<Object[]>();
        for (News n : news) {
            if (n.getId() == null)
                continue;
            LOGGER.info("Start save news of id {}", n.getId());
            Object[] objects = {n.getId(), n.getTitle(),
                    n.getContent(), n.getTime(), n.getContentUrl(),n.getEditor(),n.getSource()};
            placeholder.add(objects);
        }
        this.batchUpdate("Insert into news VALUES(?,?,?,?,?,?,?)", placeholder);
    }


}
