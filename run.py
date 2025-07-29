import csv
import psycopg2
import requests
import logging
import configparser
from psycopg2.extras import execute_values

logger = logging.getLogger("DataImport")
logging.basicConfig(filename='dataimport.log', encoding='utf-8', level=logging.DEBUG)

class DataImport:
  base_url = "https://api.igdb.com/v4/dumps/"
  #endpoints = ["games","companies","covers","genres","keywords","platforms","release_dates"]
  endpoints = ["games"]
  column_names = [
    "id", "name", "slug", "url", "created_at", "updated_at", "summary", "storyline",
    "collection", "franchise", "franchises", "hypes", "follows", "rating",
    "aggregated_rating", "aggregated_rating_count", "total_rating", "total_rating_count",
    "rating_count", "parent_game", "version_parent", "version_title", "similar_games",
    "tags", "game_engines", "category", "player_perspectives", "game_modes", "keywords",
    "themes", "genres", "expansions", "dlcs", "bundles", "standalone_expansions",
    "first_release_date", "status", "platforms", "release_dates", "alternative_names",
    "screenshots", "videos", "cover", "websites", "external_games", "multiplayer_modes",
    "involved_companies", "age_ratings", "artworks", "checksum", "remakes", "remasters",
    "expanded_games", "ports", "forks", "language_supports", "game_localizations",
    "collections", "game_status", "game_type"
  ]

  client_id = ""
  client_secret = ""
  download_destination = ""
  token_url = ""
  access_token = ""
  db_username = ""
  db_pass = ""
  db_host = ""
  db_name = ""

  def __init__(self):
    config = configparser.ConfigParser()
    config.read("dataimport.ini")
    self.client_id = config["api"]["client_id"]
    self.client_secret = config["api"]["client_secret"]
    self.download_destination = config["localpaths"]["download_destination_path"]
    self.token_url = f"https://id.twitch.tv/oauth2/token?client_id={self.client_id}&client_secret={self.client_secret}&grant_type=client_credentials"
    self.db_username = config["database"]["username"]
    self.db_pass = config["database"]["password"]
    self.db_host = config["database"]["host"]
    self.db_name = config["database"]["dbname"]

  def get_token(self):
    rsp = requests.post(self.token_url)
    if rsp.status_code == 200:
      json = rsp.json()
      self.access_token = json["access_token"]
      logger.info(f"SUCCESS: Access Token Retrieved: {self.access_token}")
      self.get_csv_from_igdb()
    else:
      logger.error(f"Requests to get access token failed with status code: {rsp.status_code}")

  def get_csv_from_igdb(self):
    headers = {
      "Authorization": f"Bearer {self.access_token}",
      "Client-ID": self.client_id
    }

    download_urls = {}

    for endpoint in self.endpoints:
      url = self.base_url + endpoint
      rsp = requests.get(url, headers=headers)
      if rsp.status_code == 200:
        logger.info(f"SUCCESS: Connected to endpoint: {endpoint}")
        json = rsp.json()
        d_url = json["s3_url"]
        download_urls[endpoint] = d_url
      else:
        logger.error(f"Request to endpoint: {endpoint}, FAILED with status code: {rsp.status_code}")
      
    for endpoint in download_urls:
      rsp = requests.get(download_urls[endpoint])
      if rsp.status_code == 200:
        filename = self.download_destination + endpoint + ".csv"
        self.write_csv(rsp.content, filename)
      else:
        logger.error(f"Failed to retrieve download link from endpoint: {endpoint}, FAILED with status code: {rsp.status_code}")

  def upsert_rows(self, conn, table_name, rows, column_names):
    with conn.cursor() as cur:
      columns = ','.join(column_names)
      sql = f"""
          INSERT INTO igdb_{table_name} ({columns})
          VALUES %s
          ON CONFLICT (id) DO UPDATE SET
            {getUpdatedString(column_names)}
          WHERE igdb_{table_name}.updated_at < EXCLUDED.updated_at
      """

      data = []

      row_count = len(rows)
      current_row = 0

      for row in rows:
        id_is_number = verifyInt(row["id"])
        if (id_is_number == False):
          logger.warning(f"Found broken record at id: {row["id"]}, skipping...")
          continue
        
        # Some rows in the csv are inconsistent. Array types sometimes have an individual integer. 
        # Below, we check if the column name is plural, and if the value is a single integer. If so, we cast to a list.
        for col in column_names:
          if (col.endswith("s") and (col != "status" and col != "hypes" and col != "game_status")):
            if row[col] in (None, "", "null", "NULL"):
              continue
            if isinstance(row[col], list):
              continue
            if isinstance(row[col], str):
              valueIsArray = row[col].endswith("}")
              if valueIsArray == False:
                row[col] = "{" + row[col] + "}"
              else:
                continue
            
        tempData = [row[col] if row[col] != "" else None for col in column_names]
        data.append(tempData)
        #TODO: row #89606 Wizard of Oz has the first_release_date value in the wrong column, resulting in an error where the statement tries to 
        # insert a timestamp where it should be inserting NULL or a integer
        # find a way to handle that.
        if (tempData[0] == '269668'):
          print("yo")
        logger.info(f"Appending row with id: {row["id"]}, row {current_row} of {row_count}")
        current_row += 1

      execute_values(cur, sql, data, page_size=5000)
      conn.commit()
    
  def run_upsert(self, csvs):
    connString = f"postgresql://{self.db_username}:{self.db_pass}@{self.db_host}/{self.db_name}"

    with psycopg2.connect(connString) as conn:
        for obj in csvs:
          self.upsert_rows(conn, obj["endpoint"], obj["file"], obj["columns"])

  def load_csvs(self):
    csvs = []
    for endpoint in self.endpoints:
      path = self.download_destination + endpoint + ".csv"
      data = self.parse_csv(path)
      columns = list(data[0].keys())
      obj = {
        "columns": columns,
        "endpoint": endpoint,
        "file": data
      }
      logger.info(f"SUCCESS: Loaded csv for {endpoint} into csvs array")
      csvs.append(obj)
    
    return csvs

  def parse_csv(self, filepath):
    with open(filepath, newline='') as f:
      return list(csv.DictReader(f))
    
  def write_csv(self, content, filepath):
    with open(filepath, mode="wb") as file:
      file.write(content)
  
  def run(self):
    #self.get_token()
    #self.get_csv_from_igdb()
    csvs = self.load_csvs()
    self.run_upsert(csvs)
    
def getParameterString(length):
  return ', '.join(['%s'] * length)

def getUpdatedString(column_names):
  return ', '.join([f"{col} = EXCLUDED.{col}" for col in column_names if col != 'id'])

def verifyInt(value):
  try:
    int(value)
    return True
  except:
    return False
  

importer = DataImport()
importer.run()