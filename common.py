import pandas as pd
import re
from collections import Counter
import nltk
from nltk.corpus import stopwords
import spacy

# Download stopwords (run once)
nltk.download('stopwords')

# Load SpaCy English model
nlp = spacy.load("en_core_web_sm")

# Load your scraped CSV
df = pd.read_csv("posts.csv")

# English stopwords
english_stops = set(stopwords.words('english'))

# Tagalog stopwords (you can expand this list)
tagalog_stops = {
    "ng", "sa", "mga", "ako", "ka", "siya", "yung", "lang",
    "din", "naman", "wala", "meron", "hindi", "oo", "nga",
    "sana", "pero", "diba", "ayun", "ah", "eh", "ba", "na"
}

# Merge stopwords
all_stops = english_stops.union(tagalog_stops)

# Whitelist for makeup adjectives (always keep if found)
makeup_adjectives = {
    "matte", "glossy", "shimmery", "dewy", "velvety",
    "creamy", "powdery", "pigmented", "blendable",
    "hydrating", "lightweight", "longlasting", "buildable",
    "buttery", "flaky", "radiant", "silky"
}

adjectives = []

# Loop through the text in the CSV
for text in df['post_content'].dropna():
    # Clean text: remove non-letters, lowercase everything
    text = re.sub(r'[^a-zA-Z ]', '', text.lower())
    doc = nlp(text)

    # Extract adjectives only
    for token in doc:
        if token.pos_ == "ADJ" and token.text not in all_stops:
            adjectives.append(token.text)

    # Always include whitelisted makeup adjectives if present
    for word in makeup_adjectives:
        if word in text.split():
            adjectives.append(word)

# Count all adjectives
adj_counts = Counter(adjectives)

# Convert to DataFrame, sort by frequency
adj_df = pd.DataFrame(adj_counts.items(), columns=["word", "count"]).sort_values(by="count", ascending=False)

# Save to CSV
adj_df.to_csv("top_adjectives.csv", index=False, encoding="utf-8")

print("✅ Saved adjectives (including makeup terms like 'matte') to top_adjectives.csv")
