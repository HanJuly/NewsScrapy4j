package com.han.scrapy.process;


import com.google.gson.Gson;
import com.han.scrapy.utils.HttpClientUtils;
import com.han.scrapy.utils.JedisUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class News163Scrapy {

//明星栏目首页url:  https://ent.163.com/special/000380VU/newsdata_star.js?callback=data_callback
//明星栏目分页url: https://ent.163.com/special/000380VU/newsdata_star_02.js?callback=data_callback
//
//电影栏目首页url:  https://ent.163.com/special/000380VU/newsdata_movie.js?callback=data_callback
//电影栏目分页url : https://ent.163.com/special/000380VU/newsdata_movie_02.js?callback=data_callback
//
//电视剧栏目首页url: https://ent.163.com/special/000380VU/newsdata_tv.js?callback=data_callback
//电视剧栏目分页url: https://ent.163.com/special/000380VU/newsdata_tv_02.js?callback=data_callback
//
//综艺栏目首页url: https://ent.163.com/special/000380VU/newsdata_show.js?callback=data_callback
//综艺栏目分页url: https://ent.163.com/special/000380VU/newsdata_show_02.js?callback=data_callback
//
//音乐栏目首页url: https://ent.163.com/special/000380VU/newsdata_music.js?callback=data_callback
//音乐栏目分页url: https://ent.163.com/special/000380VU/newsdata_music_02.js?callback=data_callback


    private final static Logger LOGGER = LoggerFactory.getLogger(News163Scrapy.class);

    private final static String DIST_URL_163_KEY = "distributed:com.han.scrapy:news:detailurl";

    private final static String DIST_URL_163_KEY_REPETITION = "distributed:com.han.scrapy:news:uniqueurl";

    private String indexUrl = "https://ent.163.com/special/000380VU/newsdata_index.js?callback=data_callback";

    private int page = 1;

    public static void main(String[] args) {
        News163Scrapy newsScrapy = new News163Scrapy();
        newsScrapy.startScapy();
    }

    public void startScapy() {
        List<String> catoryUrls = catoryBuild();
        for (String catoryUrl : catoryUrls) {
            LOGGER.info("Acquire module of index url {}......................", catoryUrl);
            indexUrl = catoryUrl;
            page = 1;
            while (true) {
                LOGGER.info("Acquire {} page data..........", page);
                if (!indexData())
                    break;
                String pageNum = getPage();
                indexUrl = catoryUrl.substring(0, catoryUrl.indexOf("js") - 1) + "_" + pageNum + ".js?callback=data_callback";

            }
        }

        LOGGER.info("Scrapy data  url end....................");
    }

    private List<String> catoryBuild() {
        List<String> catoryUrls = new ArrayList<String>();
        catoryUrls.add("https://ent.163.com/special/000380VU/newsdata_index.js?callback=data_callback");
        catoryUrls.add("https://ent.163.com/special/000380VU/newsdata_star.js?callback=data_callback");
        catoryUrls.add("https://ent.163.com/special/000380VU/newsdata_movie.js?callback=data_callback");
        catoryUrls.add("https://ent.163.com/special/000380VU/newsdata_tv.js?callback=data_callback");
        catoryUrls.add("https://ent.163.com/special/000380VU/newsdata_show.js?callback=data_callback");
        catoryUrls.add("https://ent.163.com/special/000380VU/newsdata_music.js?callback=data_callback");
        return catoryUrls;
    }

    private String getPage() {
        String num = "";
        page++;
        if (page < 10) {
            num = "0" + page;
        } else {
            num = "" + page;
        }
        return num;
    }


    private boolean indexData() {
        Jedis jedis = JedisUtils.getJedis();
        List newsList = new ArrayList();
        try {
            String content = HttpClientUtils.doGet(indexUrl);
            if (content == null) {
                LOGGER.error("Invalid url {}:", indexUrl);
                return false;
            }
            String temp = content.substring(content.indexOf("(") + 1, content.lastIndexOf(")"));
            Gson gson = new Gson();
            List<Map<String, String>> jsonObj = gson.fromJson(temp, List.class);
            for (Map<String, String> map : jsonObj) {
                String contentUrl = map.get("docurl");

                if (!contentUrl.contains("ent.163.com") || contentUrl.contains("photoview")) {
                    continue;
                }
                if (!jedis.sismember(DIST_URL_163_KEY_REPETITION, contentUrl)) {

                    jedis.lpush(DIST_URL_163_KEY, contentUrl);
                    jedis.sadd(DIST_URL_163_KEY_REPETITION,contentUrl);
                }
                LOGGER.info("Write contentUrl of {} into redis by distributed deploy!", contentUrl);

            }
        } catch (IOException e) {
            LOGGER.error("Acquire index data failed: {}", e.getMessage());
        }
        jedis.close();
        LOGGER.info("Acquire news of size {}", newsList.size());
        return true;
    }
}
