kafkacat -b beta-hbase02:9092,beta-hbase03:9092,beta-hbase04:9092 \
        -t crawldrv_grpc_test \
        -D? \
        -P <<EOF
{
  "CrawlCategoryEnum": 0,
  "Myasync": true,
  "UserAgent": "Mozilla/5.0 (X11; Linux x86_64)    AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36",
  "Parallelism": 4,
  "RandomDelay": 5,
  "Url2Crawl": "https://finance.sina.com.cn/forex/",
  "NewsEntry": "div[class^=main-content]",
  "Queries2Extract":[
    "h1[class=main-title]",
    "div[class=top-bar-wrap]>div[class^='top-bar ani']>div[class='top-bar-inner clearfix']>div[class=date-source]>span[class=date]",
    "div[class=top-bar-wrap]>div[class^='top-bar ani']>div[class='top-bar-inner clearfix']>div[class=date-source]>a[href]",
    "div[class^='article-content clearfix']>div[class=article-content-left]>div[class=article][id=artibody]>p"
  ],
  "ScriptPublishedAt": "ret = strings.replacer(\"年\", \"-\", \"月\", \"-\", \"日\", \"\").replace(\"%s\")",
  "Logflag": 2
};
EOF
