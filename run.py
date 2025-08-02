import csv
import psycopg2
import requests
import logging
import configparser
from datetime import datetime
import uuid
import re
from psycopg2.extras import execute_values

logger = logging.getLogger("DataImport")
logging.basicConfig(filename='dataimport.log', encoding='utf-8', level=logging.DEBUG)

true_values = {"t", "true", "yes", "y", "1", "x", "on", "enabled", "active", "✓", "✔"}
false_values = {"f", "false", "no", "n", "0", "", "off", "disabled", "inactive", "none", "null"}

class DataImport:
  base_url = "https://api.igdb.com/v4/dumps/"
  endpoints = ["games","companies","covers","genres","keywords","platforms"]
  filters = {
    "games": {
      "themes": [42], # filter out erotic games
      "game_type": [5] # filter out mods
    }
  }

  column_names_with_types = {
    "games": {
      "id": "int",
      "name": "text",
      "slug": "text",
      "url": "text",
      "created_at": "timestamp",
      "updated_at": "timestamp",
      "summary": "text",
      "storyline": "text",
      "collection": "int",
      "franchise": "int",
      "franchises": "int_array",
      "hypes": "int",
      "follows": "int",
      "rating": "float",
      "aggregated_rating": "float",
      "aggregated_rating_count": "int",
      "total_rating": "float",
      "total_rating_count": "int",
      "rating_count": "int",
      "parent_game": "int",
      "version_parent": "int",
      "version_title": "text",
      "similar_games": "int_array",
      "tags": "int_array",
      "game_engines": "int_array",
      "category": "int",
      "player_perspectives": "int_array",
      "game_modes": "int_array",
      "keywords": "int_array",
      "themes": "int_array",
      "genres": "int_array",
      "expansions": "int_array",
      "dlcs": "int_array",
      "bundles": "int_array",
      "standalone_expansions": "int_array",
      "first_release_date": "timestamp",
      "status": "int",
      "platforms": "int_array",
      "release_dates": "int_array",
      "alternative_names": "int_array",
      "screenshots": "int_array",
      "videos": "int_array",
      "cover": "int",
      "websites": "int_array",
      "external_games": "int_array",
      "multiplayer_modes": "int_array",
      "involved_companies": "int_array",
      "age_ratings": "int_array",
      "artworks": "int_array",
      "checksum": "uuid",
      "remakes": "int_array",
      "remasters": "int_array",
      "expanded_games": "int_array",
      "ports": "int_array",
      "forks": "int_array",
      "language_supports": "int_array",
      "game_localizations": "int_array",
      "collections": "int_array",
      "game_status": "int",
      "game_type": "int"
    },
    "covers": {
      "id": "int",
      "url": "text",
      "image_id": "text",
      "width": "int",
      "height": "int",
      "alpha_channel": "bool",
      "animated": "bool",
      "game": "int",
      "checksum": "uuid",
      "game_localization": "int"
    },
    "companies": {
      "id": "int",
      "name": "text",
      "created_at": "timestamp",
      "updated_at": "timestamp",
      "slug": "text",
      "url": "text",
      "logo": "int",
      "description": "text",
      "start_date": "timestamp",
      "start_date_category": "int",
      "country": "int",
      "parent": "int",
      "changed_company_id": "int",
      "change_date": "timestamp",
      "change_date_category": "int",
      "twitter": "text",
      "facebook": "text",
      "published": "int_array",
      "developed": "int_array",
      "website": "int",
      "websites": "int_array",
      "checksum": "uuid",
      "status": "int",
      "start_date_format": "int",
      "change_date_format": "int"
    },
    "genres": {
      "id": "int",
      "name": "text",
      "created_at": "timestamp",
      "updated_at": "timestamp",
      "slug": "text",
      "url": "text",
      "checksum": "uuid"
    },
    "keywords": {
      "id": "int",
      "name": "text",
      "created_at": "timestamp",
      "updated_at": "timestamp",
      "slug": "text",
      "url": "text",
      "checksum": "uuid"
    },
    "platforms": {
      "id": "int",
      "name": "text",
      "slug": "text",
      "url": "text",
      "created_at": "timestamp",
      "updated_at": "timestamp",
      "summary": "text",
      "category": "int",
      "platform_family": "int",
      "alternative_name": "text",
      "generation": "int",
      "versions": "int_array",
      "abbreviation": "text",
      "platform_logo": "int",
      "websites": "int_array",
      "checksum": "uuid",
      "platform_type": "int"
    }
  }

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
  
  def run(self):
    self.get_token()
    self.get_csv_from_igdb()
    sheets = self.load_csvs()
    valid_data = self.validateData(sheets)
    self.run_upsert(valid_data)

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
      use_updated = "updated_at" in column_names
      columns = ','.join(column_names)
      sql = f"""
          INSERT INTO igdb_{table_name} ({columns})
          VALUES %s
          ON CONFLICT (id) DO UPDATE SET
            {getUpdatedString(column_names)}
          {f"WHERE igdb_{table_name}.updated_at < EXCLUDED.updated_at" if use_updated else ""}
      """
      data = []
      row_count = len(rows)
      current_row = 1
      logger.info(getUpdatedString(column_names))
      for row in rows:         
        data.append([row[col] if row[col] != "" else None for col in column_names])
        logger.info(f"Appending row with id: {row["id"]}, row {current_row} of {row_count}")
        current_row += 1

      execute_values(cur, sql, data, page_size=5000)
      conn.commit()
    
  def run_upsert(self, sheets):
    connString = f"postgresql://{self.db_username}:{self.db_pass}@{self.db_host}/{self.db_name}"

    with psycopg2.connect(connString) as conn:
        for sheet in sheets:
          self.upsert_rows(conn, sheet["endpoint"], sheet["file"], sheet["columns"])

  def load_csvs(self):
    csvs = []
    for endpoint in self.endpoints:
      path = self.download_destination + endpoint + ".csv"
      data = self.parse_csv(path)
      sheet_columns = list(data[0].keys())
      columns = [f"{col}" for col in sheet_columns if col != '' and col != None]
      sheet = {
        "columns": columns,
        "endpoint": endpoint,
        "file": data
      }
      logger.info(f"SUCCESS: Loaded csv for {endpoint} into csvs array")
      csvs.append(sheet)
    
    return csvs

  def parse_csv(self, filepath):
    with open(filepath, newline='') as f:
      return list(csv.DictReader(f))
    
  def write_csv(self, content, filepath):
    with open(filepath, mode="wb") as file:
      file.write(content)

  def validateData(self, sheets):
    for sheet in sheets:
      endpoint = sheet["endpoint"]
      columns_with_types = self.column_names_with_types[endpoint]
      column_names = columns_with_types.keys()

      rows = sheet["file"]

      current_row = 1
      num_rows = len(rows)

      invalid_rows = []
      valid_rows = []

      filter_types = []

      filter_active = toggleFilter(self, endpoint)
      if filter_active:
        filter_types = self.filters[endpoint].keys()

      for row in rows:
        current_id = row["id"]
        if current_id in (None, "", "None", "null"):
          continue

        logger.info(f"Validating Data Contained in row id: {current_id}, count: {current_row} of {num_rows} rows.")
        current_row += 1

        try:
          for col in column_names:
            data_type = columns_with_types[col]
            value = row[col]

            parsed = None

            match data_type:
              case "int":
                parsed = parse_integer(value, col, current_id)
              case "text":
                parsed = parse_string(value, col, current_id)
              case "timestamp":
                parsed = parse_date(value, col, current_id)
              case "int_array":
                parsed = parse_integer_array(value, col, current_id)
              case "float":
                parsed = parse_float(value, col, current_id)
              case "uuid":
                parsed = parse_uuid(value, col, current_id)
              case "bool":
                bool_val = parse_bool(value, col, current_id)
                row[col] = bool_val
                parsed = bool_val
              case _:
                logger.info("Invalid data type found in column")
            
            if filter_active and col in filter_types:
              filter_values = self.filters[endpoint][col]
              found_match = find_common(parsed, filter_values)
              if found_match:
                logger.info(f"Found filterable row with id: {current_id}, due to {col} with value of or containing {parsed}. Removing row")
                raise Exception

          row.pop('', None)
          valid_rows.append(row)
        except:
          invalid_rows.append(row)
          continue

      logger.info(f"Valid Rows: {len(valid_rows)}")
      logger.info(f"Invalid Rows: {len(invalid_rows)}")

      sheet["file"] = valid_rows
    
    return sheets
  
def find_common(a, b):
    # Convert to sets, wrapping single values in a list
    set_a = set(a) if isinstance(a, (list, set, tuple)) else {a}
    set_b = set(b) if isinstance(b, (list, set, tuple)) else {b}

    common = set_a & set_b
    return True if common else False

def getUpdatedString(column_names):
  return ', '.join([f"{col} = EXCLUDED.{col}" for col in column_names if col != 'id' and col != '' and col != None])

def toggleFilter(self, endpoint):
  filters = self.filters.keys()
  if endpoint in filters:
    return True
  else:
    return False
  
def parse_integer(value, col, id):
  if value in (None, "", "None", "null"):
    return
  
  try:
    return int(value)
  except:
    logger.info(f"Found bad data at id: {id}, for column name: {col}, for data type: int")
    raise Exception

def parse_string(value, col, id):
  if value in (None, "", "None", "null"):
    return
  
  try:
    return str(value)
  except:
    logger.info(f"Found bad data at id: {id}, for column name: {col}, for data type: text")
    raise Exception

def parse_integer_array(value, col, id):
  if value in (None, "", "None", "null"):
    return
  
  is_valid = bool(re.fullmatch(r"\{\s*(-?\d+\s*,\s*)*-?\d*\s*\}", value.strip()))

  if not is_valid:
    logger.info(f"Found bad data at id: {id}, for column name: {col}, for data type: int_array")
    raise Exception
  
  nums = value.strip("{}").split(",")
  return [int(n.strip()) for n in nums if n.strip()]

def parse_date(value, col, id):
  if value in (None, "", "None", "null"):
    return
  
  errorsThrown = 0

  for fmt in ("%Y-%m-%d", "%b %d, %Y", "%Y-%m-%dT%H:%M:%SZ"):
    try:
      return datetime.strptime(value, fmt)
    except ValueError:
      errorsThrown += 1

  if errorsThrown == 2:
    logger.info(f"Found bad data at id: {id}, for column name: {col}, for data type: timestamp")
    raise Exception
  else:
    return

def parse_uuid(value, col, id):
  if value in (None, "", "None", "null"):
    return

  try:
    return str(uuid.UUID(value))  # Returns canonical string version
  except ValueError:
    logger.info(f"Found bad data at id: {id}, for column name: {col}, for data type: uuid")
    raise Exception

def parse_float(value, col, id):
  if value in (None, "", "None", "null"):
    return

  try:
    return float(value)
  except ValueError:
    logger.info(f"Found bad data at id: {id}, for column name: {col}, for data type: float")
    raise Exception
  
def parse_bool(value, col, id):
  if value in (None, ""):
    return False
  
  if value in true_values:
    return True
  if value in false_values:
    return False
  
  logger.info(f"Found bad data at id: {id}, for column name: {col}, for data type: bool")
  raise Exception

importer = DataImport()
importer.run()