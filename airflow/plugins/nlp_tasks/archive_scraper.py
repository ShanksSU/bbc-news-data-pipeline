import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient, errors

BBC_ARCHIVE_INDEX = "https://www.bbc.com/sitemaps/https-index-com-archive.xml"
MONGO_URI = "mongo"
MONGO_DB = "bbcnews"
MONGO_COLLECTION = "links"

def get_mongo_collection():
    # get mongo collection handle
    client = MongoClient(MONGO_URI, 27017)
    return client[MONGO_DB][MONGO_COLLECTION]

def fetch_xml(url, timeout=10):
    # download xml with timeout
    try:
        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        print(f"[error] failed to fetch {url}: {e}")
        return None

def parse_single_sitemap(xml_str):
    # parse one sitemap xml and extract urls
    soup = BeautifulSoup(xml_str, "lxml")
    loc_elements = soup.find_all("url")

    results = []
    for loc in loc_elements:
        try:
            url = loc.loc.text
            lastmod = loc.lastmod.text
            if "www.bbc.com/news/" in url:
                results.append({"lastmod": lastmod, "url": url})
        except Exception:
            continue

    return results

def insert_links_bulk(records):
    # bulk insert urls into mongo
    if not records:
        return 0

    col = get_mongo_collection()

    inserted = 0
    for rec in records:
        try:
            col.insert_one(rec)
            inserted += 1
        except errors.DuplicateKeyError:
            continue

    return inserted

def collect_archive_sitemaps():
    # main scraper for bbc archive sitemaps
    print("[archivescraper] fetching index sitemap...")
    index_xml = fetch_xml(BBC_ARCHIVE_INDEX)
    if not index_xml:
        return 0

    soup = BeautifulSoup(index_xml, "lxml")
    sitemap_urls = [loc.text for loc in soup.find_all("loc")]

    total_inserted = 0

    for i, sitemap_url in enumerate(sitemap_urls, start=1):
        print(f"[archivescraper] [{i}/{len(sitemap_urls)}] {sitemap_url}")
        xml = fetch_xml(sitemap_url)
        if not xml:
            continue

        records = parse_single_sitemap(xml)
        inserted = insert_links_bulk(records)
        total_inserted += inserted

        print(f"  → inserted {inserted} new urls")

    print(f"[archivescraper] completed. total new urls inserted: {total_inserted}")
    return total_inserted
