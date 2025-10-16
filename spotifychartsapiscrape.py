from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import os
import glob
import pandas as pd
import pyodbc
import os
# ---- CONFIG ----
#output directory of csv
download_dir = r""
os.makedirs(download_dir, exist_ok=True)
date = "2025-08-28"
spotify_countries = {
    "pk": "Pakistan",
    "pa": "Panama",
    "py": "Paraguay",
    "pe": "Peru",
    "ph": "Philippines",
    "pl": "Poland",
    "pt": "Portugal",
}

# spotify_countries = { "global": "Global"}
# spotify_countries = { "ae": "United Arab Emirates","tr": "Turkey","se": "Sweden", "us": "United States",
#                       "sk": "Slovakia","sg": "Singapore", "ve": "Venezuela","gb": "United Kingdom",
#                       "vn": "Vietnam", "kr": "South Korea", "ch": "Switzerland", "th": "Thailand",
#                       "tw": "Taiwan","uy": "Uruguay", "ua": "Ukraine","es": "Spain",
#                       "za": "South Africa",}
# spotify_countries = {
#     "ar": "Argentina", "au": "Australia", "at": "Austria", "by": "Belarus", "be": "Belgium",
#     "bo": "Bolivia", "br": "Brazil", "bg": "Bulgaria", "ca": "Canada", "cl": "Chile",
#     "co": "Colombia", "cr": "Costa Rica", "cy": "Cyprus", "cz": "Czech Republic",
#     "dk": "Denmark", "do": "Dominican Republic", "ec": "Ecuador", "eg": "Egypt",
#     "sv": "El Salvador", "ee": "Estonia", "fi": "Finland", "fr": "France", "de": "Germany",
#     "gr": "Greece", "gt": "Guatemala", "hn": "Honduras", "hk": "Hong Kong", "hu": "Hungary",
#     "is": "Iceland", "in": "India", "id": "Indonesia", "ie": "Ireland", "il": "Israel",
#     "it": "Italy", "jp": "Japan", "kz": "Kazakhstan", "lv": "Latvia", "lt": "Lithuania",
#     "lu": "Luxembourg", "my": "Malaysia", "ua": "Ukraine", "gb": "United Kingdom",
#     "uy": "Uruguay", "us": "United States", "ve": "Venezuela", "vn": "Vietnam",
#     "ro": "Romania", "sa": "Saudi Arabia", "sg": "Singapore", "sk": "Slovakia",
#     "za": "South Africa", "kr": "South Korea", "es": "Spain", "se": "Sweden",
#     "ch": "Switzerland", "tw": "Taiwan", "th": "Thailand", "tr": "Turkey",
#     "ae": "United Arab Emirates", "mx": "Mexico", "ma": "Morocco", "nl": "Netherlands",
#     "nz": "New Zealand", "ni": "Nicaragua", "ng": "Nigeria", "no": "Norway"
# }
files = glob.glob(os.path.join(download_dir, "*"))

# Delete each file
for f in files:
    try:
        os.remove(f)
        print(f"Deleted: {f}")
    except Exception as e:
        print(f"Error deleting {f}: {e}")

# ---- SETUP CHROME ----
options = webdriver.ChromeOptions()
prefs = {
    "download.default_directory": download_dir,
    "download.prompt_for_download": False,
    "directory_upgrade": True,
}
options.add_experimental_option("prefs", prefs)
driver = webdriver.Chrome(options=options)

driver.get("https://charts.spotify.com/")
print("👉 Please log in manually in the browser window...")
time.sleep(40)  # Give yourself time to log in

# ---- DOWNLOAD CSVs ----
for country_code, country_name in spotify_countries.items():
    chart_url = f"https://charts.spotify.com/charts/view/regional-{country_code}-weekly/{date}"
    driver.get(chart_url)
    time.sleep(15)

    try:
        button = driver.find_element(By.XPATH, "//button[@aria-labelledby='csv_download']")

        # Clear old files first
        before_download = set(os.listdir(download_dir))

        # Click download
        button.click()

        # Wait for the new file to appear
        timeout = 10
        while timeout > 0:
            time.sleep(1)
            after_download = set(os.listdir(download_dir))
            new_files = after_download - before_download
            if new_files:
                downloaded_file = new_files.pop()
                old_path = os.path.join(download_dir, downloaded_file)
                new_path = os.path.join(download_dir, f"{country_name}.csv")
                os.rename(old_path, new_path)
                print(f"✅ Downloaded and renamed: {new_path}")
                break
            timeout -= 1
        else:
            print(f"⚠️ Timeout: File not downloaded for {country_name}")

    except Exception as e:
        print(f"⚠️ Error downloading {country_name}: {e}")

driver.quit()

# ---- POST-PROCESS CSVs ----
csv_files = sorted(glob.glob(os.path.join(download_dir, "*.csv")))
all_dfs = []

for file in csv_files:
    df = pd.read_csv(file)
    country_name = os.path.splitext(os.path.basename(file))[0]

    # Insert 'country_name' and 'date' after the last existing column
    last_col_index = len(df.columns)
    df.insert(last_col_index, "country_name", country_name)
    df.insert(last_col_index + 1, "date", date)

    all_dfs.append(df)
    df.to_csv(file, index=False)

# ---- COMBINE ALL CSVs ----
master_csv_path = os.path.join(download_dir, f"spotify_charts_master_{date}.csv")
master_df = pd.concat(all_dfs, ignore_index=True)
master_df.to_csv(master_csv_path, index=False)
print(f"✅ All CSVs processed. Master CSV: {master_csv_path}")

# ---- INSERT INTO SQL SERVER ----
# Change these to your SQL Server credentials
server = os.environ["SERVER"]
database = os.environ["DATABASE"]
table_name = os.environ["TABLE_NAME"]

conn = pyodbc.connect(
    f"DRIVER={{SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;TrustServerCertificate=True;"
)
cursor = conn.cursor()




file_count = 0
# List all entries in the folder
for entry in os.listdir(download_dir):
    # Create the full path to the entry
    full_path = os.path.join(download_dir, entry)
    # Check if the entry is a file and not a directory
    if os.path.isfile(full_path):
        file_count += 1



for df in all_dfs:
    for _, row in df.iterrows():
        cursor.execute(f"""
            INSERT INTO {table_name} 
            (rank, uri, artist_names, track_name, source, peak_rank, previous_rank, weeks_on_chart, streams, country_name, date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        row['rank'],
        row['uri'],
        row['artist_names'],
        row['track_name'],
        row['source'],
        row['peak_rank'],
        row['previous_rank'],
        row['weeks_on_chart'],
        row['streams'],
        row['country_name'],
        row['date']
        )
    conn.commit()


cursor.close()
conn.close()
print("✅ All CSVs inserted into SQL Server successfully.")
