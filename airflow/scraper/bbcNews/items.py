import scrapy

class BbcnewsItem(scrapy.Item):
    # define fields for scraped bbc news articles
    url = scrapy.Field()
    date = scrapy.Field()
    title = scrapy.Field()
    subtitle = scrapy.Field()
    authors = scrapy.Field()
    text = scrapy.Field()
    topic_name = scrapy.Field()
    topic_url = scrapy.Field()
    images = scrapy.Field()