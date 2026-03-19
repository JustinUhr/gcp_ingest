import os
from pathlib import Path, PureWindowsPath
from argparse import ArgumentParser
from pprint import pformat
from dotenv import load_dotenv
from ingest import ingest_files
from create_streams import queue_create_stream_job
import logging
import pandas as pd
import json

stream_map = {
  ".mov": "VIDEO-MASTER",
  ".mp4": "VIDEO-MASTER",
  ".pdf": "PDF",
}
cache = {}

# Column name for matching transcripts/translations to videos
VIDEO_PARENT_COLUMN = 'parent'

def abbr_path(path:str, length:int, sep:str='/',abbr_len:int=2):
  if len(path) < length:
    return path

  split_path = [part for part in path.split(sep) if part]
  abbr_split_path = [part[:abbr_len] for part in split_path]

  test_path = ""
  if sep == '/':
    test_path += sep

  for i in range(len(split_path)):
    test_path = sep.join(abbr_split_path[:i])+sep+sep.join(split_path[i:])
    if len(test_path) > length:
      continue
    return test_path
  return '...'+test_path[length-3:]

def get_cache_options(split_path:list):
  # logging.debug(f"Getting cache options from split path {split_path}")
  cache_options = []
  for i in range(len(split_path), 0, -1):
    cache_options.append('\\'.join(split_path[:i+1]))
  return cache_options

def get_mnt_path_from_windows_path(windows_path:str, cache={'mntdir':{'path':Path('/mnt')}}):
  sep='\\'
  logging.debug(f"Getting mnt path from windows path {abbr_path(windows_path,40,sep)}")
  if windows_path in cache:
    return cache[windows_path]['path']

  winpath = PureWindowsPath(windows_path)
  split_path = windows_path.split('\\')
  cache_options = get_cache_options(split_path)

  if winpath.drive:
    # Old format: Z:\path\... → mntdir/z/path/...
    new_root = cache['mntdir']['path'].joinpath(winpath.drive[0].lower())
    parts_start = 1  # skip drive component
  else:
    # New format: files22.brown.edu\path\... → mntdir/path/...
    new_root = cache['mntdir']['path']
    parts_start = 1  # skip server name

  for option in cache_options:
    if option in cache:
      remaining_path = '\\'.join(split_path[len(option.split('\\')):])
      return cache[option]['path'].joinpath(remaining_path)
    else:
      filepath = new_root.joinpath(*option.split('\\')[parts_start:])
      cache[option] = {'path':filepath}

  cache[windows_path] = {'path': filepath}

  return filepath

def dict_from_row(row, pid=None):
  logging.debug(f"Creating dict from row {row.get('identifierFileName')}")
  # Get the filepath from the row and replace the drive letter
  filepath_str = row['filepath']
  filepath = get_mnt_path_from_windows_path(filepath_str,cache)
  # add path to cache
  cache[filepath_str]['path'] = filepath
  filename = str(row['identifierFileName']).strip()

  if not filepath.exists():
    logging.warning(f"File {filepath} does not exist")
    return {}

  if filepath.is_dir():
    # Old format: filepath is a directory, glob for filename
    if not cache[filepath_str].get('glob', None):
      cache[filepath_str]['glob'] = list(filepath.glob('*'))
    fileglob = cache[filepath_str]['glob']
    logging.debug(f"Fileglob: {fileglob}")
    files = file_from_glob(filename, fileglob, allowed_streams=stream_map.keys())
    if len(files) == 0:
      logging.warning(f"No files found for {filename} in {filepath}")
      return {}
    if len(files) > 1:
      logging.warning(f'Multiple files found for {filename}: {files}')
      return {}
    found_file = files[0]
  else:
    # New format: filepath is the file itself
    if filepath.stem != filename:
      logging.warning(f"Filename mismatch: expected {filename}, got {filepath.stem}")
      return {}
    found_file = filepath

  result_dict = {
    'filepath': found_file,
    'filename': filename,
  }

  # Determine document type - extension first, then title for PDFs
  if found_file.suffix.lower() in ['.mov', '.mp4']:
    result_dict['doc_type'] = 'video'
  elif found_file.suffix.lower() == '.pdf':
    title = row.get('itemTitle', '').lower()
    if 'transcript' in title:
      result_dict['doc_type'] = 'transcript'
      result_dict['video_parent'] = row.get(VIDEO_PARENT_COLUMN, '').strip()
    elif 'translation' in title:
      result_dict['doc_type'] = 'translation'
      result_dict['video_parent'] = row.get(VIDEO_PARENT_COLUMN, '').strip()
    else:
      result_dict['doc_type'] = 'other_pdf'
      logging.debug(f"Treating {filename} as other_pdf (title: {row.get('itemTitle', '')})")
  else:
    logging.error(f"Unsupported file type for {filename}: {found_file.suffix}")
    raise ValueError(f"Unsupported file type: {found_file.suffix}")

  if pid:
    result_dict.update({
      "pid": pid,
      "children": []
    })
  return result_dict

def file_from_glob(filename, fileglob,allowed_streams=[]):
  files = []
  for file in fileglob:
    if filename == file.name:
      return [file]
    if file.stem != filename:
      logging.debug(f"Skipping file {file.name} because {file.stem} != {filename}")
      continue
    if allowed_streams and file.suffix.lower() not in allowed_streams:
      logging.debug(f"Skipping {file.suffix[1:].upper()} file {file.name}")
      continue
    logging.debug(f"Found {file.suffix[1:].upper()} file {file.name}")
    files.append(file)
  return files

def match_transcript_to_video(transcript_filename, videos):
  if len(videos) == 1:
    return videos[0]
  # Try to match on first segment before underscore e.g. C0008_French -> C0008 matches C0008_AF
  transcript_prefix = transcript_filename.split('_')[0]
  matches = [v for v in videos if v['filename'].split('_')[0] == transcript_prefix]
  if len(matches) == 1:
    return matches[0]
  if len(matches) > 1:
    logging.error(f"Multiple video matches for {transcript_filename}: {[v['filename'] for v in matches]}")
    return None
  logging.error(f"No video match for {transcript_filename}. Videos: {[v['filename'] for v in videos]}")
  return None

def make_ingestable(data: pd.DataFrame):
  logging.info("Making data ingestable")

  if VIDEO_PARENT_COLUMN not in data.columns:
    raise ValueError(f"Required column '{VIDEO_PARENT_COLUMN}' not found in spreadsheet. "
                     f"Available columns: {list(data.columns)}")

  data_dict = data.to_dict('records')
  data_dict.pop(0)
  logging.debug([
      { "parent":row['parent'],
        "filename":row['identifierFileName']
      } for row in data_dict[:4]
  ])

  parented_data = []
  for row in data_dict:
    if row.get('ingestcomplete') and not row.get('pid'):
      logging.debug(f'ingest already completed for {row["itemTitle"]}')
      continue
    if not row['identifierFileName'] or not row["filepath"]:
      logging.warning(f"Row has no filename and/or path: {row['itemTitle']}")
      continue
    # Skip rows that have a parent - they'll be collected as children below
    if row[VIDEO_PARENT_COLUMN] and type(row[VIDEO_PARENT_COLUMN]) is str:
      continue

    root_filename = str(row['identifierFileName']).strip()

    if row.get("pid"):
      # Parent already ingested - collect any uningest children
      for child in data_dict:
        if child.get('ingestcomplete'):
          logging.debug(f"ingest already completed for {child['itemTitle']}")
          continue
        if not child['identifierFileName']:
          continue
        if child[VIDEO_PARENT_COLUMN] == root_filename:
          parented_data.append(dict_from_row(child, row['pid']))
      continue

    # Root video row becomes first child of synthesized metadata-only parent
    root_video_dict = dict_from_row(row)
    children = [root_video_dict] if root_video_dict else []

    for child in data_dict:
      if not child['identifierFileName']:
        continue
      if child[VIDEO_PARENT_COLUMN] == root_filename:
        child_dict = dict_from_row(child)
        if child_dict:
          children.append(child_dict)

    parent = {
      "filename": root_filename,
      "filepath": None,
      'children': children,
    }
    parented_data.append(parent)

  logging.debug(pformat(parented_data,sort_dicts=False,))
  return parented_data

def get_progress_file(data_file, sheet):
  stem = Path(data_file).stem
  return Path(f'../{stem}_{sheet}_progress.json')

def load_progress(progress_file):
  if progress_file.exists():
    with open(progress_file) as f:
      logging.info(f'Loaded progress from {progress_file}')
      return json.load(f)
  return {}

def save_progress(progress_file, progress):
  with open(progress_file, 'w') as f:
    json.dump(progress, f, indent=2)
  logging.debug(f'Saved progress to {progress_file}')

def ingest_data(data, mods_dir, collection_pid, progress_file):
  logging.info("Ingesting data")
  progress = load_progress(progress_file)

  for item in data:
    if not item:
      continue
    filename = item['filename'].strip()
    children = item.get('children', [])

    # Categorize children by type
    videos = [c for c in children if c.get('doc_type') == 'video']
    transcripts = [c for c in children if c.get('doc_type') == 'transcript']
    translations = [c for c in children if c.get('doc_type') == 'translation']
    other_pdfs = [c for c in children if c.get('doc_type') == 'other_pdf']

    videos.sort(key=lambda v: v['filename'])

    # Validate transcript/translation matching before starting any ingestion
    for transcript in transcripts:
      if not match_transcript_to_video(transcript['filename'], videos):
        raise ValueError(f"Could not match transcript {transcript['filename']} to a video")
    for translation in translations:
      if not match_transcript_to_video(translation['filename'], videos):
        raise ValueError(f"Could not match translation {translation['filename']} to a video")

    # Create or resume parent
    if filename in progress:
      parent_pid = progress[filename]['pid']
      logging.info(f'Resuming: found existing parent {parent_pid} for {filename}')
    else:
      mods = Path(mods_dir).joinpath(f'{filename}.mods.xml')
      logging.info(f'Ingesting parent item {filename}')
      parent_pid = ingest_files(mods, None, stream_map, collection_pid)
      if not parent_pid:
        raise RuntimeError(f"Failed to create parent item {filename}")
      progress[filename] = {'pid': parent_pid, 'children': {}}
      save_progress(progress_file, progress)
      logging.info(f'Created parent {parent_pid}')

    parent_progress = progress[filename]['children']

    # Ingest videos
    video_info = {}
    for i, video in enumerate(videos, start=1):
      if video['filename'] in parent_progress:
        video_pid = parent_progress[video['filename']]
        logging.info(f'Resuming: found existing video {video_pid} for {video["filename"]}')
      else:
        video_mods = Path(mods_dir).joinpath(f'{video["filename"]}.mods.xml')
        logging.info(f'Ingesting video {video["filename"]} as page {i}')
        video_pid = ingest_files(
          video_mods,
          video['filepath'],
          stream_map,
          collection_pid,
          parent_relationship=(parent_pid, 'isPartOf'),
          page_number=i
        )
        if not video_pid:
          raise RuntimeError(f"Failed to create video item {video['filename']}")
        parent_progress[video['filename']] = video_pid
        save_progress(progress_file, progress)
        logging.info(f'Created video {video_pid}, queuing stream job')
        queue_create_stream_job(video_pid)

      video_info[video['filename']] = {'pid': video_pid, 'page_num': i}

    # Ingest transcripts
    for transcript in transcripts:
      if transcript['filename'] in parent_progress:
        logging.info(f'Resuming: skipping already ingested transcript {transcript["filename"]}')
        continue
      video = match_transcript_to_video(transcript['filename'], videos)
      v_info = video_info[video['filename']]
      page_num = f"{v_info['page_num']}a"
      transcript_mods = Path(mods_dir).joinpath(f'{transcript["filename"]}.mods.xml')
      logging.info(f'Ingesting transcript {transcript["filename"]} as page {page_num}')
      transcript_pid = ingest_files(
        transcript_mods,
        transcript['filepath'],
        stream_map,
        collection_pid,
        parent_relationship=(parent_pid, 'isPartOf'),
        page_number=page_num,
        additional_parents=[v_info['pid']],
        transcript_of=v_info['pid']
      )
      if not transcript_pid:
        raise RuntimeError(f"Failed to create transcript {transcript['filename']}")
      parent_progress[transcript['filename']] = transcript_pid
      save_progress(progress_file, progress)
      logging.info(f'Created transcript {transcript_pid}')

    # Ingest translations
    for translation in translations:
      if translation['filename'] in parent_progress:
        logging.info(f'Resuming: skipping already ingested translation {translation["filename"]}')
        continue
      video = match_transcript_to_video(translation['filename'], videos)
      v_info = video_info[video['filename']]
      page_num = f"{v_info['page_num']}b"
      translation_mods = Path(mods_dir).joinpath(f'{translation["filename"]}.mods.xml')
      logging.info(f'Ingesting translation {translation["filename"]} as page {page_num}')
      translation_pid = ingest_files(
        translation_mods,
        translation['filepath'],
        stream_map,
        collection_pid,
        parent_relationship=(parent_pid, 'isPartOf'),
        page_number=page_num,
        additional_parents=[v_info['pid']]
      )
      if not translation_pid:
        raise RuntimeError(f"Failed to create translation {translation['filename']}")
      parent_progress[translation['filename']] = translation_pid
      save_progress(progress_file, progress)
      logging.info(f'Created translation {translation_pid}')

    # Ingest other PDFs
    for i, pdf in enumerate(other_pdfs):
      if pdf['filename'] in parent_progress:
        logging.info(f'Resuming: skipping already ingested PDF {pdf["filename"]}')
        continue
      page_num = chr(ord('a') + i)
      pdf_mods = Path(mods_dir).joinpath(f'{pdf["filename"]}.mods.xml')
      logging.info(f'Ingesting other PDF {pdf["filename"]} as page {page_num}')
      pdf_pid = ingest_files(
        pdf_mods,
        pdf['filepath'],
        stream_map,
        collection_pid,
        parent_relationship=(parent_pid, 'isPartOf'),
        page_number=page_num
      )
      if not pdf_pid:
        raise RuntimeError(f"Failed to create PDF {pdf['filename']}")
      parent_progress[pdf['filename']] = pdf_pid
      save_progress(progress_file, progress)
      logging.info(f'Created other PDF {pdf_pid}')

def check_ingestable_for_mods(data, mods_dir):
  logging.info("Checking data for MODS files")
  for item in data:
    if not item:
      continue
    filename = item['filename'].strip()

    mods = Path(mods_dir).joinpath(f'{filename}.mods.xml')
    if not mods.exists():
        logging.warning(f"mods {mods.name} does not exist")

    if "children" not in item.keys():
      logging.warning(f"item has no key 'children': {item}")
      continue

    for child in item['children']:
      if not child:
        continue
      mods = Path(mods_dir).joinpath(f'{child["filename"]}.mods.xml')
      if not mods.exists():
        logging.warning(f"mods {mods.name} does not exist")

def get_sheet_name(filepath):
  sheets = pd.ExcelFile(filepath).sheet_names
  for i, sheet in enumerate(sheets):
    print(i,sheet)
  sheet_num = int(input("Enter the number of the sheet you want to ingest: "))
  return sheets[sheet_num]

def check_cols(filepath,sheet_name=None):
  with open(filepath, 'rb') as f:
    # print names of sheets
    if not sheet_name:
      sheet_name = get_sheet_name(filepath)
    data = pd.read_excel(f,sheet_name)
    # Remove empty rows
    data.dropna(how='all', inplace=True)
    data.fillna('',inplace=True)
    # Check for empty column headers in pandas dataframe
    headers = data.columns
    # logging.debug(f"Headers: {headers}")
    second_row = data.iloc[0]
    for i, header in enumerate(headers):
      if 'Unnamed' in header:
        if "parent" in second_row[i].lower():
          data.rename(columns={header: 'parent'}, inplace=True)
          continue
        if "filepath" in second_row[i].lower():
          data.rename(columns={header: 'filepath'}, inplace=True)
          continue
        print(f"Column {i + 1} is missing, second row value is {second_row[i]}")
        new_header = input(f"Enter the column header for column {i + 1}: ")
        if not new_header.isidentifier():
          raise ValueError(f"'{new_header}' is not a valid column header")
        data.rename(columns={header: new_header}, inplace=True)
    return data

def main(args):
  load_dotenv()
  mods_dir = os.environ['MODS_DIR']
  sheet = check_cols(args.data_file, args.sheet)
  data = make_ingestable(sheet)
  if args.mock:
    check_ingestable_for_mods(data, mods_dir)
    logging.debug(pformat(data,sort_dicts=False))
    logging.info("Mock run, not ingesting")
    return
  progress_file = get_progress_file(args.data_file, args.sheet)
  ingest_data(data, mods_dir, args.collection, progress_file)

def parse_arguments():
  parser = ArgumentParser()
  parser.add_argument('collection',
    type=str,
    help='PID of the collection to ingest into (e.g. bdr:12345)'
  )
  parser.add_argument('data_file',
    type=Path,
    help='Path to the data file'
  )
  parser.add_argument('--mntdir',
    type=str,
    default='/mnt',
    help='Parent dir of mount(s), win drive letter is used as actual mountpoint'
  )
  parser.add_argument('--sheet',
    type=str,
    help='Sheet name in the excel file'
  )
  parser.add_argument('--mock',
    action='store_true',
    help='Run without ingesting'
  )
  parser.add_argument("-l", "--log",
    dest="loglevel",
    choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
    default='INFO',
    help="Set the logging level")
  args = parser.parse_args()
  return args

if __name__ == '__main__':
  args = parse_arguments()
  logging.basicConfig(
    level=args.loglevel,
    format='[%(asctime)s] %(levelname)s [%(module)s-%(funcName)s()::%(lineno)d] %(message)s',
    datefmt='%d/%b/%Y %H:%M:%S',
    handlers=[
        logging.FileHandler("../gcp_ingest.log"),
        logging.StreamHandler()
    ]
  )
  mount_dirpath = Path(args.mntdir)
  cache.update({
    'mntdir': {'path':mount_dirpath},
    args.mntdir: {'path':mount_dirpath}
  })
  main(args)