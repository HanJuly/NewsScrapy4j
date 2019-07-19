package com.han.scrapy.process;

import com.google.gson.Gson;
import com.han.scrapy.pojo.News;
import com.han.scrapy.utils.HttpClientUtils;
import com.han.scrapy.utils.IdWorker;
import com.han.scrapy.utils.JedisUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class NewsScrapyStep2 {



    private final static Logger LOGGER = LoggerFactory.getLogger(NewsScrapyStep2.class);

    private final static String DIST_URL_163_KEY = "distributed:com.han.scrapy:news:detailurl";

    private final static String DIST_JSON_163_KEY = "distributed:com.han.scrapy:news:json";

    private IdWorker idWorker = new IdWorker();

    public static void main(String[] args) {
        NewsScrapyStep2 newsScrapy = new NewsScrapyStep2();
        newsScrapy.startScapy();
    }

    public void startScapy() {
        LOGGER.info("Start Scrapy data from redis data.....................");
       acquireDetails();
        LOGGER.info("Scrapy data end....................");
    }


    private void acquireDetails() {
        Jedis jedis = JedisUtils.getJedis();

        while (true){
            List<String> contentUrl = jedis.brpop(20,DIST_URL_163_KEY);
            if (contentUrl == null || contentUrl.size() == 0){
                LOGGER.info("Redis detail url is work out.");
                break;
            }
            LOGGER.info("Scrapy data from url {}.",contentUrl.get(1));
            try {
                String html = HttpClientUtils.doGet(contentUrl.get(1));
                Document document = Jsoup.parse(html);
//#epContentLeft > h1
                Elements title = document.select("#epContentLeft > h1");
//                #endText
                Elements content = document.select("#endText");
//                #epContentLeft > div.post_time_source
                Elements time = document.select("#epContentLeft > div.post_time_source");
                String timeAndSource = time.text();                String[] split = timeAndSource.split("　来源: ");
                Elements spanEl = document.select(".ep-editor");
                String editor = spanEl.text();
                editor = editor.substring(editor.indexOf("：")+1,editor.lastIndexOf("_"));
                News n = new News();
                n.setId(Long.valueOf(idWorker.nextId()).toString());
                n.setTitle(title.text());
                n.setTime(split[0]);
                n.setContent(content.text());
                n.setEditor(editor);
                n.setSource(split[1]);
                Gson gson = new Gson();
                String newsJson = gson.toJson(n);
                jedis.lpush(DIST_JSON_163_KEY,newsJson);

            } catch (Exception e) {
                LOGGER.error("Acquire details data failed: {}", e.getMessage());

            }
        }
        jedis.close();
    }


}
