import logging
from pathlib import Path
import os
import shutil
from dotenv import load_dotenv, find_dotenv
import requests
import json

def setup_environment():
  """Updates sys.path and reads the .env settings.
  Called by do_ingest."""
  logging.info("setting up environment")
  ## allows bdr_tools to be imported ------------------------------
  load_dotenv(find_dotenv())
  # Get the COLLECTION_PID from the .env file
  COLLECTION_PID = os.environ["COLLECTION_PID"]
  logging.debug(f"COLLECTION_PID, ``{COLLECTION_PID}``")
  API_IDENTITY = os.environ["API_IDENTITY"]
  logging.debug(f"API_IDENTITY, ``{API_IDENTITY}``")
  API_URL = os.environ["API_URL"]
  logging.debug(f"API_URL, ``{API_URL}``")
  OWNER_ID = os.environ["OWNER_ID"]
  logging.debug(f"OWNER_ID, ``{OWNER_ID}``")
  API_KEY = os.environ["API_KEY"]
  logging.debug(f"API_KEY, ``{API_KEY}``")

  env_vars = {}
  env_vars["collection_pid"] = COLLECTION_PID
  env_vars["api_identity"] = API_IDENTITY
  env_vars["api_url"] = API_URL
  env_vars["owner_id"] = OWNER_ID
  env_vars["api_key"] = API_KEY

  return env_vars

def set_basic_params(env_vars, additional_rights='BDR_PUBLIC#discover,display'):
  params = {
    "identity": env_vars["api_identity"],
    "authorization_code": env_vars["api_key"],
    "rights": json.dumps(
      {
        "parameters": {
          "owner_id": env_vars["owner_id"],
          "additional_rights": additional_rights
        }
      }
    ),
    "rels": json.dumps({"isMemberOfCollection": env_vars["collection_pid"]}),
  }
  return params

class TempStagingPath:
  def __init__(self,path):
    path = Path(path)
    self.srcpath = path

  def __enter__(self,*args,**kwargs):
    logging.debug(f'{args=}')
    logging.debug(f'{kwargs=}')
    if not self.srcpath.exists():
      logging.error(f"path {self.srcpath} doesn't exist")
      raise FileNotFoundError

    staging_dir = Path(os.environ['STAGING_DIR'])
    newpath = staging_dir.joinpath(self.srcpath.name)

    shutil.copyfile(self.srcpath,newpath)
    self.path = newpath
    return self.path

  def __exit__(self,*args,**kwargs):
    logging.debug(f'{args=}')
    logging.debug(f'{kwargs=}')
    self.path.unlink()

def perform_post(api_url, data, files=None):
  logging.info("performing post")
  try:
    if files:
      r = requests.post(api_url, data=data, files=files)
    else:
      r = requests.post(api_url, data=data)
  except Exception as e:
    logging.exception(f"error creating object: {e}")
    raise
  if r.ok:
    logging.debug("r is ok")
    return r.json()["pid"]
  else:
    msg = f"error creating metadata object: {r.status_code} - {r.text}"
    logging.error(msg)
    raise Exception(msg)

VIDEO_EXTENSIONS = [".mov", ".mp4", ".avi", ".mkv"]

def ingest_files(
    mods_path,
    file_path,
    allowed_streams:dict,
    parent_relationship=None,
    page_number=None,
    additional_parents=None,
    transcript_of=None
  ) -> str:
  """
  Ingests files into a system.
  Args:
    mods_path (str): The path to the MODS file.
    file_path (str): The path to the file to ingest.
    allowed_streams (dict): A dictionary mapping file extensions to content streams.
    parent_relationship (tuple): The pid and relationship to parent. Defaults to None.
    page_number (str|int): Page number/order for the item. Defaults to None.
    additional_parents (list): Additional parent PIDs for dual parentage. Defaults to None.
    transcript_of (str): PID this item is a transcript of (sets isTranscriptOf). Defaults to None.
  Returns:
    (str): The PID of the ingested files.
  """

  env_vars = setup_environment()

  if not parent_relationship:
    additional_rights = 'BDR_PUBLIC#discover,display'
  elif file_path and Path(file_path).suffix.lower() in VIDEO_EXTENSIONS:
    additional_rights = ''
  else:
    additional_rights = 'BDR_PUBLIC#display'

  params = set_basic_params(env_vars, additional_rights=additional_rights)

  mods_path = Path(mods_path)
  if not mods_path.exists():
    logging.warning(f"mods file {mods_path.name} does not exist. skipping...")
    return
  with open(mods_path, "r") as mods_file:
    mods_file_obj = mods_file.read()

  params["mods"] = json.dumps({"xml_data": mods_file_obj})

  if not parent_relationship:
    pid = perform_post(api_url=env_vars["api_url"], data=params)
    return pid

  (parent_pid, rel_type) = parent_relationship
  if rel_type not in ['isPartOf', 'isTranslationOf', 'isTranscriptOf']:
    raise ValueError(f"Invalid relationship type: {rel_type}")
  
  # Read params['rels'] into a dict
  temp_rels = json.loads(params["rels"])

  # Build parent list (primary + additional)
  parent_pids = [parent_pid]
  if additional_parents:
    parent_pids.extend(additional_parents)
  temp_rels[rel_type] = ','.join(parent_pids)

  # Set page number if provided
  if page_number is not None:
    temp_rels['page_number'] = str(page_number)

  # Set transcript relationship (separate from isPartOf parentage)
  if transcript_of:
    temp_rels['isTranscriptOf'] = transcript_of

  # Convert params['rels'] back to a string
  params["rels"] = json.dumps(temp_rels)

  if not file_path:
    logging.warning(f"While ingesting, there's no file path for {mods_path.name}")
    raise TypeError
  logging.debug(f"ingesting {file_path}")

  file = Path(file_path)
  content_streams = []

  if allowed_streams and file.suffix.lower() not in allowed_streams.keys():
    logging.warning(f"File extension {file.suffix} not allowed. skipping...")
    return
  # params["content_model"] = allowed_streams[file.suffix]

  with TempStagingPath(file) as newpath:
    content_streams.append({
      "dsID": allowed_streams[file.suffix.lower()],
      "file_name": file.name,
      "path":str(newpath)
    })
    params['content_streams'] = json.dumps(content_streams)

    logging.debug(f"{params=}")
    logging.debug(f"{content_streams=}")

    pid = perform_post(api_url=env_vars["api_url"], data=params)

    return pid

if __name__ == "__main__":
  logging.info("__name__ is `main`")