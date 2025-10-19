import praw
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()
reddit = praw.Reddit(
    client_id=os.environ["CLIENT_ID"],
    client_secret=os.environ["CLIENT_SECRET"],
    user_agent=os.environ["USER_AGENT"]
)

subreddit = reddit.subreddit('beautytalkph')

# ---------------------
makeup_categories = [
    "foundation", "foundations",
    "concealer", "concealers",
    "primer", "primers",
    "powder", "powders",
    "blush", "blushes",
    "highlighter", "highlighters",
    "bronzer", "bronzers",
    "contour", "contours",
    "setting spray", "setting sprays",
    "fixing spray", "fixing sprays",

    "eyeshadow", "eyeshadows",
    "eyeliner", "eyeliners",
    "mascara", "mascaras",
    "eyebrow", "brow gel", "brow",
    "lashes", "lash",
    "eye primer", "eye primers",
    "palette", "palettes",

    "lipstick", "lipsticks",
    "lip gloss", "lip glosses",
    "lip liner", "lip liners",
    "lip balm", "lip balms",
    "lip stain", "lip stains",
    "skin tint", "skin tints",

    "brush", "brushes",
    "sponge", "sponges",
    "eyelash curler", "eyelash curlers",
    "tweezers", "tweezer",
    "makeup remover", "makeup removers",

    "bb cream", "bb creams",
    "cc cream", "cc creams",
    "dd cream", "dd creams",
    "moisturizer", "moisturizers",
    "makeup",
    "color corrector", "color correctors"
]

# ---------------------
local_brands = [
    "ever bilena", "blk", "colourette", "filipinta beauty", "lip pinas",
    "clochefame", "cloud", "beach born", "sunnies", "belo",
    "happy skin", "issy", "lucky beauty", "rhode", "popique", "vice", "dr. sensitive",
    "grwm", "teviant", "avon", "cuco", "enigma", "ellana", "careline", "armani",
    "shawill", "miniso", "kkv"
]

international_brands = [
    "maybelline", "mac", "fenty", "loreal", "nars", "revlon",
    "clinique", "laura mercier", "canmake", "peripera", "romand",
    "carolina herrera", "marc jacobs", "maison margiela", "cacharel",
    "monotheme", "etude house", "laneige", "clio", "missha",
    "sace lady", "jmcy",

    "3ce", "bbia", "heimish", "the saem", "apieu", "moonshot", "espoir",
    "tony moly", "vdl", "holika holika", "hince", "dasique",

    "shiseido", "kate", "opera", "excel", "majolica majorca",
    "visee", "cezanne", "chifure", "kiss me", "kose",

    "florasis", "perfect diary", "catkin", "judydoll",
    "into you", "zenn", "colorkey", "mao ge ping", "carslan", "hold live"
]

# Combine all brands for detection
all_brands = local_brands + international_brands

# ---------------------
def detect_brands(text):
    found = [brand for brand in all_brands if brand.lower() in text.lower()]
    return ", ".join(found) if found else None

def detect_brand_source(brand_text):
    if not brand_text:
        return None
    sources = []
    for brand in brand_text.split(", "):
        if brand in local_brands:
            sources.append("local")
        elif brand in international_brands:
            sources.append("international")
    return ", ".join(sources) if sources else None

def detect_category(text):
    found = [cat for cat in makeup_categories if cat.lower() in text.lower()]
    return ", ".join(found) if found else None

# NEW: detect if TikTok, Shopee, Lazada are mentioned
def detect_platforms(text):
    platforms = []
    for platform in ["tiktok", "shopee", "lazada"]:
        if platform in text.lower():
            platforms.append(platform)
    return ", ".join(platforms) if platforms else None

# ---------------------
# Scrape data
posts_data = []
comments_data = []

# Collect posts
for submission in subreddit.top(limit=1000):

    post_content = submission.title
    if submission.selftext:
        post_content += " " + submission.selftext

    brands = detect_brands(post_content)
    category = detect_category(post_content)
    platforms = detect_platforms(post_content)

    if brands and category:
        posts_data.append({
            'post_id': submission.id,
            'post_content': post_content,
            'post_url': submission.url,
            'post_upvotes': submission.score,
            'post_comments': submission.num_comments,
            'flair': submission.link_flair_text,
            'brand_found': brands,
            'brand_source': detect_brand_source(brands),
            'category_mentioned': category,
            'platform_mentioned': platforms,
            'date': datetime.utcfromtimestamp(submission.created_utc).strftime('%Y-%m-%d')
        })

# Collect comments
for submission in subreddit.top(limit=1000):

    submission.comments.replace_more(limit=0)
    for comment in submission.comments.list():

        comment_body = comment.body
        brands_comment = detect_brands(comment_body)
        category_comment = detect_category(comment_body)
        platforms_comment = detect_platforms(comment_body)

        if brands_comment and category_comment:
            comments_data.append({
                'post_id': submission.id,
                'comment_id': comment.id,
                'comment_body': comment_body,
                'comment_upvotes': comment.score,
                'brand_found': brands_comment,
                'brand_source': detect_brand_source(brands_comment),
                'category_mentioned': category_comment,
                'platform_mentioned': platforms_comment,
                'date': datetime.utcfromtimestamp(comment.created_utc).strftime('%Y-%m-%d')
            })

# ---------------------
# Convert to DataFrames and save
posts_df = pd.DataFrame(posts_data)
comments_df = pd.DataFrame(comments_data)

posts_df.to_csv('posts.csv', index=False)
comments_df.to_csv('comments.csv', index=False)

print("Scraping complete: posts.csv and comments.csv")
