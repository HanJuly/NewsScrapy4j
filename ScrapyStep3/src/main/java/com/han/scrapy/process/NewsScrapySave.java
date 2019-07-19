package com.han.scrapy.process;

import com.google.gson.Gson;
import com.han.scrapy.dao.NewsDao;
import com.han.scrapy.pojo.News;
import com.han.scrapy.utils.JedisUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.Arrays;
import java.util.List;

public class NewsScrapySave {

    private final static Logger LOGGER = LoggerFactory.getLogger(NewsScrapySave.class);

    private final static String DIST_JSON_163_KEY = "distributed:com.han.scrapy:news:json";

    private final static String DIST_URL_163_KEY_REPETITION = "distributed:com.han.scrapy:news:uniqueurl";


    public static void main(String[] args) {
        NewsScrapySave newsScrapySave = new NewsScrapySave();
        newsScrapySave.startScapy();
    }

    public void startScapy() {
        LOGGER.info("Start save data....................");
        Jedis jedis = JedisUtils.getJedis();
        NewsDao newsDao = new NewsDao();
        while (true) {
            List<String> list = jedis.brpop(20, DIST_JSON_163_KEY);
            if (list == null || list.size() == 0) {
                LOGGER.info("Redis data is workout.");
                break;
            }
            Gson gson = new Gson();
            News news = gson.fromJson(list.get(1), News.class);
            newsDao.saveData(Arrays.asList(news));
        }
        jedis.close();

        LOGGER.info("Save data end....................");
    }

}
