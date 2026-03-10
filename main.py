import os
from pathlib import Path, PureWindowsPath
from argparse import ArgumentParser
from pprint import pformat
from dotenv import load_dotenv
from ingest import ingest_files
from create_streams import queue_create_stream_job
import logging
import pandas as pd

stream_map = {
  ".mov": "VIDEO-MASTER",
  ".mp4": "VIDEO-MASTER",
  ".pdf": "PDF",
}
cache = {}

# Column name for matching transcripts/translations to videos
VIDEO_PARENT_COLUMN = 'Parent Filename of the access interview file'

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
  new_root = cache['mntdir']['path'].joinpath(winpath.drive[0].lower())

  split_path = windows_path.split('\\')
  cache_options = get_cache_options(split_path)
  # logging.debug(f"Cache options: {cache_options}")

  for option in cache_options:
    if option in cache:
      # get the remaining path after the cached path
      remaining_path = '\\'.join(split_path[len(option.split('\\')):])
      return cache[option]['path'].joinpath(remaining_path)
    else:
      filepath = new_root.joinpath(*option.split('\\')[1:])
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
  if not filepath.is_dir():
    logging.warning(f"File {filepath} is not a directory")
    return {}

  if not cache[filepath_str].get('glob', None):
    cache[filepath_str]['glob'] = list(filepath.glob('*'))
  fileglob = cache[filepath_str]['glob']
  logging.debug(f"Fileglob: {fileglob}")
  files = file_from_glob(filename, fileglob,allowed_streams=stream_map.keys())

  if len(files) == 0:
    logging.warning(f"No files found for {filename} in {filepath}")
    return {}
  if len(files) > 1:
    logging.warning(f'Multiple files found for {filename}: {files}')
    return {}

  result_dict = {
    'filepath': files[0],
    'filename': filename,
  }

  # Determine document type based on genre and file extension
  genre = row.get('genreAAT', '')
  if 'transcriptions (documents)' in genre:
    result_dict['doc_type'] = 'transcript'
    result_dict['video_parent'] = row.get(VIDEO_PARENT_COLUMN, '').strip()
  elif 'translations (documents)' in genre:
    result_dict['doc_type'] = 'translation'
    result_dict['video_parent'] = row.get(VIDEO_PARENT_COLUMN, '').strip()
  elif files[0].suffix.lower() in ['.mov', '.mp4']:
    result_dict['doc_type'] = 'video'
  else:
    result_dict['doc_type'] = 'other_pdf'

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

def make_ingestable(data: pd.DataFrame):
  logging.info("Making data ingestable")

  # Validate required column exists
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
    if row['parent'] and type(row['parent']) is str:
      continue

    if row.get("pid"):
      for child in data_dict:
        if child.get('ingestcomplete'):
          logging.debug(f"ingest already completed for {child['itemTitle']}")
          continue
        if not child['identifierFileName']:
          continue
        if child['parent'] == row['identifierFileName']:
          parented_data.append(dict_from_row(child,row['pid']))
      continue

    # New parent item - gather all children
    children = []
    for child in data_dict:
      if not child['identifierFileName']:
        continue
      if child['parent'] == row['identifierFileName']:
        child_dict = dict_from_row(child)
        if child_dict:
          children.append(child_dict)

    parent = {
      "filename": row['identifierFileName'],
      "filepath": None,
      'children': children,
    }
    parented_data.append(parent)

  logging.debug(pformat(parented_data,sort_dicts=False,))
  return parented_data

def ingest_data(data, mods_dir):
  logging.info("Ingesting data")
  for item in data:
    if not item:
      continue
    filename = item['filename'].strip()
    parent_pid = item.get('pid', None)

    if parent_pid:
      # Item already has a parent PID - this is adding to existing parent
      # TODO: Handle this case with new logic if needed for resuming partial ingests
      filepath = item['filepath']
      mods = Path(mods_dir).joinpath(f'{filename}.mods.xml')
      logging.info(f'Ingesting {filename} with existing parent {parent_pid}')
      ingest_files(mods, filepath, stream_map, (parent_pid, 'isPartOf'))
      continue

    # New parent item - process with full workflow
    children = item.get('children', [])

    # Categorize children by type
    videos = [c for c in children if c.get('doc_type') == 'video']
    transcripts = [c for c in children if c.get('doc_type') == 'transcript']
    translations = [c for c in children if c.get('doc_type') == 'translation']
    other_pdfs = [c for c in children if c.get('doc_type') == 'other_pdf']

    # Sort videos by filename to establish order
    videos.sort(key=lambda v: v['filename'])

    # Validate transcript/translation video references before starting
    video_filenames = {v['filename'] for v in videos}
    for transcript in transcripts:
      video_parent = transcript.get('video_parent')
      if not video_parent:
        raise ValueError(f"Transcript {transcript['filename']} missing '{VIDEO_PARENT_COLUMN}' value")
      if video_parent not in video_filenames:
        raise ValueError(f"Transcript {transcript['filename']} references video '{video_parent}' "
                         f"which was not found. Known videos: {sorted(video_filenames)}")
    for translation in translations:
      video_parent = translation.get('video_parent')
      if not video_parent:
        raise ValueError(f"Translation {translation['filename']} missing '{VIDEO_PARENT_COLUMN}' value")
      if video_parent not in video_filenames:
        raise ValueError(f"Translation {translation['filename']} references video '{video_parent}' "
                         f"which was not found. Known videos: {sorted(video_filenames)}")

    # Create parent item first
    mods = Path(mods_dir).joinpath(f'{filename}.mods.xml')
    logging.info(f'Ingesting parent item {filename}')
    parent_pid = ingest_files(mods, None, stream_map)
    if not parent_pid:
      logging.error(f"Ingest failed, no pid for parent {filename}")
      raise RuntimeError(f"Failed to create parent item {filename}")
    logging.info(f'Created parent {parent_pid}')

    # Track video info for matching transcripts/translations
    video_info = {}  # filename -> {pid, page_num}

    # Ingest videos with page numbers
    for i, video in enumerate(videos, start=1):
      video_mods = Path(mods_dir).joinpath(f'{video["filename"]}.mods.xml')
      logging.info(f'Ingesting video {video["filename"]} as page {i}')
      video_pid = ingest_files(
        video_mods,
        video['filepath'],
        stream_map,
        parent_relationship=(parent_pid, 'isPartOf'),
        page_number=i
      )
      if not video_pid:
        logging.error(f"Ingest failed for video {video['filename']}")
        raise RuntimeError(f"Failed to create video item {video['filename']}")

      video_info[video['filename']] = {'pid': video_pid, 'page_num': i}
      logging.info(f'Created video {video_pid}, queuing stream job')

      # Queue stream creation - stream will inherit page_number and parent from video
      queue_create_stream_job(video_pid)

    # Ingest transcripts
    for transcript in transcripts:
      video_filename = transcript.get('video_parent')
      video = video_info[video_filename]
      page_num = f"{video['page_num']}a"

      transcript_mods = Path(mods_dir).joinpath(f'{transcript["filename"]}.mods.xml')
      logging.info(f'Ingesting transcript {transcript["filename"]} as page {page_num}')
      transcript_pid = ingest_files(
        transcript_mods,
        transcript['filepath'],
        stream_map,
        parent_relationship=(parent_pid, 'isPartOf'),
        page_number=page_num,
        additional_parents=[video['pid']],
        transcript_of=video['pid']
      )
      if not transcript_pid:
        raise RuntimeError(f"Failed to create transcript {transcript['filename']}")
      logging.info(f'Created transcript {transcript_pid}')

    # Ingest translations
    for translation in translations:
      video_filename = translation.get('video_parent')
      video = video_info[video_filename]
      page_num = f"{video['page_num']}b"

      translation_mods = Path(mods_dir).joinpath(f'{translation["filename"]}.mods.xml')
      logging.info(f'Ingesting translation {translation["filename"]} as page {page_num}')
      translation_pid = ingest_files(
        translation_mods,
        translation['filepath'],
        stream_map,
        parent_relationship=(parent_pid, 'isPartOf'),
        page_number=page_num,
        additional_parents=[video['pid']]
        # Note: no transcript_of for translations
      )
      if not translation_pid:
        raise RuntimeError(f"Failed to create translation {translation['filename']}")
      logging.info(f'Created translation {translation_pid}')

    # Ingest other PDFs
    for i, pdf in enumerate(other_pdfs):
      page_num = chr(ord('a') + i)  # a, b, c, ...

      pdf_mods = Path(mods_dir).joinpath(f'{pdf["filename"]}.mods.xml')
      logging.info(f'Ingesting other PDF {pdf["filename"]} as page {page_num}')
      pdf_pid = ingest_files(
        pdf_mods,
        pdf['filepath'],
        stream_map,
        parent_relationship=(parent_pid, 'isPartOf'),
        page_number=page_num
      )
      if not pdf_pid:
        raise RuntimeError(f"Failed to create PDF {pdf['filename']}")
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
  ingest_data(data, mods_dir)

def parse_arguments():
  parser = ArgumentParser()
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