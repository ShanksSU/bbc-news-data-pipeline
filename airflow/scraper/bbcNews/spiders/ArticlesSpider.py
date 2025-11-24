import scrapy
from pymongo import MongoClient
from bbcNews.items import BbcnewsItem


class NewsSpider(scrapy.Spider):
    name = "NewsSpider"
    _mongo_client = None

    @staticmethod
    def get_mongo_db():
        # get mongo db client (singleton)
        if NewsSpider._mongo_client is None:
            NewsSpider._mongo_client = MongoClient("mongo", 27017)
        return NewsSpider._mongo_client["bbcnews"]

    def __init__(self, docs_count=None, *args, **kwargs):
        # init spider and load urls from db
        super().__init__(*args, **kwargs)

        try:
            docs_count = int(docs_count) if docs_count is not None else 500
        except ValueError:
            docs_count = 500

        db = self.get_mongo_db()

        # fetch latest article urls
        cursor = (
            db.links.find(
                {"url": {"$regex": r"^https://www\.bbc\.com/news/articles/"}}
            )
            .sort("lastmod", -1)
            .limit(docs_count)
        )
        self.start_urls = [doc["url"] for doc in cursor]

        self.logger.info(
            f"🔎 loaded {len(self.start_urls)} start urls (bbc news article pages)"
        )

    def parse(self, response):
        # parse article page
        item = BbcnewsItem()
        item["url"] = response.url
        item["date"] = response.css("time::attr(datetime)").get()

        # extract title
        title_selectors = [
            "div[data-component='headline-block'] h1::text",
            "h1[data-testid='headline']::text",
            "h1#main-heading::text",
        ]

        item["title"] = next(
            (response.css(sel).get().strip() for sel in title_selectors if response.css(sel).get()),
            None,
        )

        # extract subtitle
        subtitle_selectors = [
            "p[data-component='summary']::text",
            "div[data-component='subheadline-block'] p::text",
            "p[b]::text",
        ]

        item["subtitle"] = next(
            (response.css(sel).get().strip() for sel in subtitle_selectors if response.css(sel).get()),
            None,
        )

        # extract authors
        author_selectors = [
            "span[data-testid='byline-name']::text",
            "p[data-component='byline'] span::text",
            "strong::text",
        ]

        for sel in author_selectors:
            authors = [a.strip() for a in response.css(sel).getall() if a.strip()]
            if authors:
                item["authors"] = authors
                break
        else:
            item["authors"] = []

        # extract text paragraphs
        paragraphs = [
            p.strip()
            for p in response.css("main p::text").getall()
            if p.strip()
        ]
        item["text"] = " ".join(paragraphs) if paragraphs else "N/A"

        # extract topics
        item["topic_name"] = [
            t.strip()
            for t in response.css("a[data-testid='topic-link']::text").getall()
            if t.strip()
        ]

        item["topic_url"] = response.css(
            "a[data-testid='topic-link']::attr(href)"
        ).getall()

        # extract images
        item["images"] = response.css("img::attr(src)").getall()

        # debug log
        self.logger.debug(f"parsed: {item['url']} (title={item['title']})")

        return item