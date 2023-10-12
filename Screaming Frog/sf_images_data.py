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
bq_images = "###" #bilder daten

credentials = service_account.Credentials.from_service_account_file(
        'key.json',
    )

def send_teams(webhook_url:str, content:str, title:str, color:str="000000") -> int:
    """
      - Send a teams notification to the desired webhook_url
      - Returns the status code of the HTTP request
        - webhook_url : the url you got from the teams webhook configuration
        - content : your formatted notification content
        - title : the message that'll be displayed as title, and on phone notifications
        - color (optional) : hexadecimal code of the notification's top line color, default corresponds to black
    """
    response = requests.post(
        url=webhook_url,
        headers={"Content-Type": "application/json"},
        json={
            "themeColor": color,
            "summary": title,
            "sections": [{
                "activityTitle": title,
                "activitySubtitle": content
            }],
        },
    )
    return response.status_code # Should be 200

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
path_internal_html = login_now("internal_html.csv", "/tmp/internal_html.csv")
path_images = login_now("internal_images.csv", "/tmp/images.csv")

### Load Dataframes
df_html = pd.read_csv(path_internal_html, low_memory=False)
df_images = pd.read_csv(path_images, low_memory=False)
print("dataframes loaded")

## Fallback falls Forbidden
if any((df_html['Address'] == 'https://www.urlaubsguru.de/') & (df_html['Status Code'] != 200)):
    title_teams = "SEO Datawarehouse Crawling | Forbidden"
    content_teams = f"""
    <h2>Check Screaming Frog Settings</h2>
    <p>Screaming Frog ist aktuell nicht in der Lage zu crawlen - https://www.urlaubsguru.de/ enthält einen Status Code ungleich 200 (3XX/4XX/5XX).</p>
    <ul>
    <li><a href='###'>Link zur SEO DWH Dokumentation</a></li>
    </ul>
    """
    url_teams_push = "###"
    send_teams(url_teams_push, content_teams, title_teams)
print("frog crawl = ok")

#verzeichnisse
columns = ["root", "directory", "page_level1", "page_level2", "page_level3", "page_level4"]

data_frames = [df_html]

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

filtered_columns = df_html.columns[df_html.columns.str.match(regex_pattern_content)]
df_html.drop(filtered_columns, axis=1, inplace=True)
print("content columns droped")

# crawl zeit ohne uhrzeit
df_html["crawl_time"] = pd.to_datetime(df_html["Crawl Timestamp"], format="%Y-%m-%d %H:%M:%S").dt.strftime("%Y-%m-%d")
df_html["Last Crawl"] = pd.to_datetime(df_html["Last Crawl"], format="%Y-%m-%d %H:%M:%S").dt.strftime("%Y-%m-%d")
print("timestamp set")

# Dokumente
df_html["doctype"] = df_html["Address"].str.contains("|".join(picture_extensions), case=False, regex=True)
df_html["doctype"] = df_html["doctype"].map({True: "Picture", False: "HTML"})
df_html_images = df_html[(df_html["doctype"] == "Picture")]
df_html = df_html[(df_html["doctype"] == "HTML")]
print("df_html is done")

### Image data
column_names = df_images.columns
filtered_columns = [col for col in column_names if re.match(regex_pattern_content, col)]
df_images.drop(filtered_columns, axis=1, inplace=True)
image_columns_to_keep = ["Address", "Status Code", "Size (bytes)"]

df_images_all = pd.concat([df_images, df_html_images], keys=["Address"])
df_images_all = df_images_all.drop(columns=set(df_images_all.columns) - set(image_columns_to_keep))
df_images_all["crawl_date"] = pd.Timestamp.now().strftime("%Y-%m-%d")
print("images are done")
### Verzeichnisse
#images
df_images_all["root"] = df_images_all["Address"].apply(lambda x: x.split('/')[2])
df_images_all["directory"] = df_images_all["Address"].apply(lambda x: x.split('/')[3] if len(x.split('/')) > 3 else None)
df_images_all["page_level1"] = df_images_all["Address"].apply(lambda x: x.split('/')[4] if len(x.split('/')) > 4 else None)
df_images_all["page_level2"] = df_images_all["Address"].apply(lambda x: x.split('/')[5] if len(x.split('/')) > 5 else None)
df_images_all["page_level3"] = df_images_all["Address"].apply(lambda x: x.split('/')[6] if len(x.split('/')) > 6 else None)
df_images_all["page_level4"] = df_images_all["Address"].apply(lambda x: x.split('/')[7] if len(x.split('/')) > 7 else None) 

# Column Renaming for GBQ
dataframes_for_renaming = [df_images_all]
for df in dataframes_for_renaming:
       df.columns = df.columns.str.replace(r'\s+|-|"', '_', regex=True).str.lower()
df_images_all.rename(columns={"size_(bytes)": "image_size"}, inplace=True)
print("renaming is done for gbq")
### To BigQuery
#appending

pandas_gbq.to_gbq(df_images_all, bq_images, project_id="###", if_exists="append", credentials=credentials)
print("df_images_all pushed to gbq")

