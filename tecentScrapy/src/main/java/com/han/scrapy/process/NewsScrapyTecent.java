package com.han.scrapy.process;

import com.google.gson.Gson;
import com.han.scrapy.pojo.News;
import com.han.scrapy.utils.HttpClientUtils;
import com.han.scrapy.utils.JedisUtils;
import org.jsoup.helper.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class NewsScrapyTecent {
    //热点url : https://pacaio.match.qq.com/irs/rcd?cid=137&token=d0f13d594edfc180f5bf6b845456f3ea&ext=ent&num=60
    //
    //非热点的首页url : https://pacaio.match.qq.com/irs/rcd?cid=146&token=49cbb2154853ef1a74ff4e53723372ce&ext=ent&page=0
    //
    //非热点的分页url :https://pacaio.match.qq.com/irs/rcd?cid=146&token=49cbb2154853ef1a74ff4e53723372ce&ext=ent&page=0
    private static final String HOT_NEWS = "https://pacaio.match.qq.com/irs/rcd?cid=137&token=d0f13d594edfc180f5bf6b845456f3ea&ext=ent&num=60";

    private static final String NORMAL_NEWS = "https://pacaio.match.qq.com/irs/rcd?cid=146&token=49cbb2154853ef1a74ff4e53723372ce&ext=ent&page=0";

    private final static Logger LOGGER = LoggerFactory.getLogger(NewsScrapyTecent.class);

    private final static String DIST_JSON_163_KEY = "distributed:com.han.scrapy:news:json";

    private final static String DIST_URL_163_KEY_REPETITION = "distributed:com.han.scrapy:news:uniqueurl";

    public static void main(String[] args) {
        NewsScrapyTecent newsScrapy2 = new NewsScrapyTecent();
        newsScrapy2.startScrapy();
    }

    private void startScrapy() {
        try {
            hotNews();
            normalNews();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void normalNews() throws IOException {
        LOGGER.info("Acquire normal news.......................");
        String url = NORMAL_NEWS;
        int i = 1;
        while (true) {
            LOGGER.info("Start page at {} num with {} ..................", i, url);
            try {
                getAndSaveNews(url);
            } catch (Exception e) {
                LOGGER.error("End com.han.scrapy with throwb:{}", e.getMessage());
                break;
            }
            url = "https://pacaio.match.qq.com/irs/rcd?cid=146&token=49cbb2154853ef1a74ff4e53723372ce&ext=ent&page=" + i;
            i++;
        }
    }

    private void hotNews() throws IOException {
        LOGGER.info("Acquire hot news .................");
        getAndSaveNews(HOT_NEWS);
    }

    private void getAndSaveNews(String url) throws IOException {
        String content = HttpClientUtils.doGet(url);
        Gson gson = new Gson();
        Map<String, Object> jsonMap = gson.fromJson(content, Map.class);
        check(content, jsonMap);
        Jedis jedis = JedisUtils.getJedis();
        List<Map<String, Object>> data = (List<Map<String, Object>>) jsonMap.get("data");
        for (Map<String, Object> map : data) {

            String contentUrl = (String) map.get("vurl");

            if (isExsit(DIST_URL_163_KEY_REPETITION, contentUrl)) {
                LOGGER.warn("Url already exist:{}", contentUrl);
                continue;
            }

            if (contentUrl.contains("video")) {
                LOGGER.warn("Url is filtered : {}", contentUrl);
                continue;
            }
            String title = (String) map.get("title");
            String time = (String) map.get("update_time");
            String cont = (String) map.get("intro");
            String source = (String) map.get("source");

            News news = new News();
            news.setId(UUID.randomUUID() + "");
            news.setTitle(title);
            news.setTime(time);
            news.setContentUrl(contentUrl);
            news.setContent(cont);
            news.setSource(source);

            String newsJson = gson.toJson(news);
            jedis.lpush(DIST_JSON_163_KEY, newsJson);

        }
        jedis.close();
    }

    private void check(String content, Map<String, Object> jsonMap) {
        double dataNum = (Double) jsonMap.get("datanum");

        if (dataNum == 0) {
            throw new RuntimeException("End com.han.scrapy data.................................");
        }

        if (StringUtil.isBlank(content)) {
            throw new RuntimeException("Content is null");
        }
    }

    public static boolean isExsit(String key, String contentUrl) {
        Jedis jedis = JedisUtils.getJedis();
        if (jedis.sismember(key, contentUrl)) {
            jedis.close();
            return true;
        } else {
            jedis.sadd(key, contentUrl);
            LOGGER.info("Write contentUrl of {} into redis.", contentUrl);
            jedis.close();
            return false;
        }
    }

}
