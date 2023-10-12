def hello_world(message):
    message = "started"
    return message

print(hello_world("message"))
import pandas as pd
import datetime
from google.oauth2 import service_account
from google.cloud import bigquery
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
import pandas_gbq
import re
import warnings

# Suppress warnings
warnings.filterwarnings("ignore")

# Variablen für BigQuery
bq_orphan = "####" #für orphan urls aus GSC Daten

credentials = service_account.Credentials.from_service_account_file(
        'key.json',
    )

def login_now(wanted_file, tmp_path):
    """
    Google Drive service with a service account.
    note: for the service account to work, you need to share the folder or
    files with the service account email.

    :return: google auth
    """
    # Define the settings dict to use a service account
    # We also can use all options available for the settings dict like
    # oauth_scope,save_credentials,etc.
    settings = {
                "client_config_backend": "service",
                "service_config": {
                       "client_json_file_path":"key.json",
                }
            }
    # Create instance of GoogleAuth
    gauth = GoogleAuth(settings=settings)
    # Authenticate
    gauth.ServiceAuth()
    drive = GoogleDrive(gauth)
    destination_file = tmp_path #"/tmp/internal_html.csv"
    source_file = wanted_file #"internal_html_piraten.csv"
    # List files in Google Drive - q ist hier der ScreamingFrog Folder
    file_list = drive.ListFile({'q': "'1xnwS1KCPfcJ31C1B3ftDWKSwSZel2rxd' in parents and trashed=false"}).GetList()

    # Find file to update
    for file1 in file_list:
        if file1['title'] == source_file:
            file_of_interest = file1
            
    
    file_of_interest.GetContentFile(destination_file)
    return destination_file

### Call Function to get ScreamingFrog Files
path_orphan_urls = login_now("search_console_orphan_urls.csv", "/tmp/orphan.csv")
path_orphan_urls_sitemap = login_now("sitemaps_orphan_urls.csv", "/tmp/orphan_sitemap.csv")

### Load Dataframes
df_orphan_urls = pd.read_csv(path_orphan_urls, low_memory=False)
df_orphan_sitemap = pd.read_csv(path_orphan_urls_sitemap, low_memory=False)
print("dataframes loaded")


#verzeichnisse
columns = ["root", "directory", "page_level1", "page_level2", "page_level3", "page_level4"]

data_frames = [df_orphan_urls, df_orphan_sitemap]

for df in data_frames:
    for column in columns:
        df[column] = df["Address"].apply(lambda x: x.split('/')[columns.index(column) + 2] if len(x.split('/')) > columns.index(column) + 2 else None)

print("directory tagging done")

### Data Cleaning
regex_pattern_content = r'^content-.*' # für content extraction
internal_pattern = r"https?://www.urlaubsguru.de.*"
whitelabel_pattern = r"(?!www\.)[a-zA-Z0-9-]+.urlaubsguru.de.*"
picture_extensions = ["jpg", "jpeg", "png", "gif", "bmp", "svg", "webp", "ico", "tiff"]
html_columns_to_keep = r"Address$|Status Code$|Title 1$|Meta Description 1$|H1-1$|Meta Robots 1|Canonical Link Element 1|Size|Word Count|Sentence Count|Average Words Per Sentence|Flesch Reading Ease Score|Readability|Text Ratio|Crawl Depth|Link Score|Unique Inlinks|Unique Outlinks|ibe_integration 1|content|number_of_deals 1|URL Inspection API Status|URL Inspection API Error|Summary|Converage|Last Crawl|Days Since Last Crawled|Crawled As|Crawl Allowed|Page Fetch|Indexing Allowed|User-Declared Canonical|Google-Selected Canonical|Mobile Usability|Rich Results|Rich Results Types|Rich Results Type Errors|Rich Results Type Warnings|crawl_time|root|directory|page_level.*|website_type|Redirect URL$|crawl_time|content$|Ahrefs"
print("variables set for data cleaning")

# orphan urls
df_all_orphans = pd.concat([df_orphan_sitemap, df_orphan_urls], keys=["Address", "Clicks"])
df_all_orphans["doctype"] = df_all_orphans["Address"].str.contains("|".join(picture_extensions), case=False, regex=True)
df_all_orphans["doctype"] = df_all_orphans["doctype"].map({True: "Picture", False: "HTML"})
df_all_orphans = df_all_orphans[(df_all_orphans["doctype"] == "HTML")]
df_all_orphans = df_all_orphans.filter(regex=r"Address|Status Code|Clicks|Impressions|CTR|Position")
df_all_orphans["crawl_date"] = pd.Timestamp.now().strftime("%Y-%m-%d")
df_all_orphans["Clicks"] = df_all_orphans["Clicks"].fillna(0, inplace=True)
df_all_orphans["Clicks"] = df_all_orphans["Clicks"].astype(float)
df_all_orphans["Impressions"] = df_all_orphans["Impressions"].fillna(0, inplace=True)
df_all_orphans["Impressions"] = df_all_orphans["Impressions"].astype(float)
df_all_orphans["CTR"] = df_all_orphans["CTR"].fillna(0, inplace=True)
df_all_orphans["CTR"] = df_all_orphans["CTR"].astype(float)
df_all_orphans["Position"] = df_all_orphans["Position"].fillna(0, inplace=True)
df_all_orphans["Position"] = df_all_orphans["Position"].astype(float)
print("df_all_orphans is done")

# Column Renaming for GBQ
dataframes_for_renaming = [df_all_orphans]
for df in dataframes_for_renaming:
       df.columns = df.columns.str.replace(r'\s+|-|"', '_', regex=True).str.lower()
        
print("renaming is done for gbq")
### To BigQuery
#appending
pandas_gbq.to_gbq(df_all_orphans, bq_orphan, project_id="###", if_exists="append", credentials=credentials)
print("df_all_orphans pushed to gbq")
