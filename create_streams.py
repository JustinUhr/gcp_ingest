import argparse
import json
import os
from rq import Queue
from redis import Redis
import requests
from dotenv import load_dotenv
import urllib.parse
from io import BytesIO
from rdflib import Graph, URIRef, Namespace

RELSEXT_NS = Namespace('info:fedora/fedora-system:def/relations-external#')
BUL_NS = Namespace('http://library.brown.edu/#')

class ResponseError(RuntimeError):
    pass

def check_response(resp,message):
    if not resp.ok:
            raise ResponseError(f"{message} - Response not ok: {resp.status_code} - {resp.headers}")
    response = resp.json()["response"]
    if not response:
        raise ResponseError(f"{message} - No response")
    return response

def queue_job(queue_name, function_name, function_args=None, function_kwargs=None):
    function_args = function_args or []
    function_kwargs = function_kwargs or {}
    q = Queue(queue_name, connection=Redis())
    return q.enqueue_call(func=function_name, args=function_args, kwargs=function_kwargs, timeout=72000)

def queue_create_stream_job(pid, datastream_or_url=None, visibility="public"):
    kwargs={'visibility': visibility}
    if datastream_or_url:
        kwargs['datastream_or_url'] = datastream_or_url
    return queue_job(queue_name='stream_objects', function_name='stream_objects.create', function_args=(pid,), function_kwargs=kwargs)

def get_top_level_items(api_url,collection):
    # Select every top level item from collection, up to 9999
    params = {
        "q":f"rel_is_member_of_collection_ssim:{collection} \
            object_type:undetermined",
        "fq":"!rel_is_part_of_ssim:['' TO *]",
        "rows":9999
    }
    response = requests.get(api_url,params)
    if not response.ok:
        print(f"Response not ok: {response.status_code} - {response.headers}")
        return
    print(f'found {response.json()["response"]["numFound"]} items...')
    return response.json()["response"]["docs"]

def get_child_with_filename(api_url,pid,filename):
    resp = requests.get(api_url,params={
        "q":f'rel_is_part_of_ssim:{pid} \
            object_type:video'
    })
    try:
        response = check_response(resp,f"{pid}, {filename}")
    except ResponseError:
        return
    if not response["docs"]:
        print(f"no docs for {pid} - {filename}")
        return
    item = response["docs"][0]

    return item

def select_stream_from_item_pid(api_url,pid):
    resp = requests.get(api_url,params={
        "q":f"rel_is_derivation_of_ssim:{pid} object_type:stream"
    })
    response = check_response(resp,f"{pid} stream")
    if response['numFound'] != 1:
        print(f"more than one stream found for {pid}")
    return response['docs']

def add_stream_to_rels(pid, panoptoId):
    params = {
        'pid':pid,
        'rels': json.dumps({'panopto_id': panoptoId}),
        'permission_ids':json.dumps([os.environ['API_IDENTITY']]),
        'message': "gcp rels ext update",
        'agent_name':"gcp ingest"
    }
    # TODO: add stream cmodel to rels... seems to need xml, see link:
    # https://github.com/Brown-University-Library/bdr_apis_project/blob/0f176eb800ca7c31b45822f291e69784d14153f7/items_app/metadata.py#L837
    r = requests.put(os.environ["API_URL"],data=params)
    if not r.ok:
        raise Exception(f'{r.status_code} - {r.text}')

def get_stream_id(pid,api_url):
    resp=requests.get(api_url,params={
        "q":"rel_is_derivation_of_ssim:"+pid
    })
    docs = check_response(resp,"").get("docs")
    if not docs:
        print(f'cant find stream for {pid}')
        return
    item = docs[0]
    panopto_id = item.get('rel_panopto_id_ssi')
    return panopto_id

def gcp_make_streams(api_url,collection):
    # create stream for all videos in collection
    resp = requests.get(api_url,params={
        "q":f"rel_is_member_of_collection_ssim:{collection} object_type:video",
        "rows":999
    })
    try:
        response = check_response(resp,"videos query")
    except ResponseError:
        print("Error on main query")
        return
    print(f"found {response['numFound']} items")

    for doc in response['docs']:
        print(f"queueing job for {doc['pid']}")
        queue_create_stream_job(doc['pid'])

def gcp_attach_streams_to_parents(api_url,collection,item_api):
    # for all parent items, attach stream id of name-matched item to parent item
    with open("../streamIDs.csv","w") as f:
            f.write("pid,status,panoptoId\n")
    parents = get_top_level_items(api_url,collection)
    if not parents:
        raise Exception("no parent items found")
    for parent in parents:
        pid = parent['pid']
        filename = parent['mods_id_filename_ssim'][0]
        matched_child = get_child_with_filename(api_url,pid,filename)
        if not matched_child:
            with open("../streamIDs.csv","a") as f:
                f.write(f"{pid},no videos,\n")
            continue
        panoptoId = get_stream_id(matched_child['pid'],api_url)
        if panoptoId:
            status="all set"
        else:
            status="no stream"
        with open("../streamIDs.csv","a") as f:
            f.write(f"{pid},{status},{panoptoId}\n")

def fetch_rels_ext(storage_url, pid):
    """Fetch current RELS-EXT from backend storage."""
    resp = requests.get(f"{storage_url}{pid}/RELS-EXT/")
    if not resp.ok:
        raise Exception(f"Failed to fetch RELS-EXT for {pid}: {resp.status_code}")
    return resp.content

def update_rels_via_xml(item_api, storage_url, pid, new_triples):
    """Fetch current RELS-EXT, add new triples, PUT back as XML."""
    rels_bytes = fetch_rels_ext(storage_url, pid)
    print(f"Current RELS-EXT for {pid}:")
    print(rels_bytes.decode())
    print('---')
    g = Graph()
    g.parse(BytesIO(rels_bytes), format='application/rdf+xml')

    obj_uri = URIRef(f'info:fedora/{pid}')
    for predicate, object_pid in new_triples:
        triple = (obj_uri, predicate, URIRef(f'info:fedora/{object_pid}'))
        if triple not in g:
            g.add(triple)

    xml_data = g.serialize(format='xml')
    print(f"Updated RELS-EXT for {pid}:")
    print(xml_data) # for debugging - shows the full XML being sent to the API
    print('---=')
    params = {
        'pid': pid,
        'rels': json.dumps({'xml_data': xml_data}),
        'permission_ids': json.dumps([os.environ['API_IDENTITY']]),
        'message': "gcp: attach stream to transcript/translation",
        'agent_name': "gcp ingest"
    }
    r = requests.put(item_api, data=params)
    if not r.ok:
        raise Exception(f'Failed to update rels for {pid}: {r.status_code} - {r.text}')

def update_item_rels(item_api, pid, rels_dict):
    """
    NOTE: This function is not currently used, but is left in case a 
    simpler approach is desired in the future. It doesn't allow for
    setting multiple isTranscriptOf values, which is why the XML approach is 
    used instead.

    Update an item's rels with the given dict.
    """
    params = {
        'pid': pid,
        'rels': json.dumps(rels_dict),
        'permission_ids': json.dumps([os.environ['API_IDENTITY']]),
        'message': "gcp: attach stream to transcript/translation",
        'agent_name': "gcp ingest"
    }
    r = requests.put(item_api, data=params)
    if not r.ok:
        raise Exception(f'Failed to update rels for {pid}: {r.status_code} - {r.text}')

def get_videos_in_collection(api_url, collection):
    """Get all video objects in the collection."""
    resp = requests.get(api_url, params={
        "q": f"rel_is_member_of_collection_ssim:{collection} object_type:video",
        "rows": 9999
    })
    response = check_response(resp, "videos in collection")
    print(f"found {response['numFound']} videos")
    return response['docs']

def get_stream_for_video(api_url, video_pid):
    """Get the stream derived from a video. Returns the stream doc or None."""
    resp = requests.get(api_url, params={
        "q": f"rel_is_derivation_of_ssim:{video_pid} object_type:stream"
    })
    try:
        response = check_response(resp, f"stream for {video_pid}")
    except ResponseError:
        return None
    if response['numFound'] == 0:
        return None
    if response['numFound'] > 1:
        print(f"WARNING: multiple streams for {video_pid}, using first")
    return response['docs'][0]

def get_transcripts_of_video(api_url, video_pid):
    """Get items that are transcripts of this video (have isTranscriptOf pointing to it)."""
    resp = requests.get(api_url, params={
        "q": f"rel_is_transcript_of_ssim:{video_pid}",
        "rows": 9999
    })
    try:
        response = check_response(resp, f"transcripts of {video_pid}")
    except ResponseError:
        return []
    return response['docs']

def get_translations_of_video(api_url, video_pid):
    """Get items that are parented to this video but are NOT transcripts of it.
    Filters to PDFs only to avoid picking up streams or other children."""
    resp = requests.get(api_url, params={
        "q": f"rel_is_part_of_ssim:{video_pid} object_type:pdf \
            !rel_is_transcript_of_ssim:{video_pid}",
        "rows": 9999
    })
    try:
        response = check_response(resp, f"translations of {video_pid}")
    except ResponseError:
        return []
    return response['docs']

def item_already_linked_to_stream(item_doc, stream_pid):
    """Check if an item already has isPartOf pointing to the stream."""
    existing_parents = item_doc.get('rel_is_part_of_ssim', [])
    return stream_pid in existing_parents

def transcript_already_linked_to_stream(item_doc, stream_pid):
    """Check if a transcript already has isTranscriptOf pointing to the stream."""
    existing = item_doc.get('rel_is_transcript_of_ssim', [])
    if isinstance(existing, str):
        existing = [existing]
    return stream_pid in existing

def build_merged_rels(existing_doc, new_rels):
    """
    Build a rels dict that merges new values with existing ones.
    Without this, we would overwrite existing rels and remove any other parents.
    existing_doc is a Solr doc dict; new_rels maps rel names to PIDs to add.
    """
    # Map from rels key to Solr field name
    solr_field_map = {
        'isPartOf': 'rel_is_part_of_ssim',
        'isTranscriptOf': 'rel_is_transcript_of_ssim',
    }
    merged = {}
    for rel_name, new_pid in new_rels.items():
        solr_field = solr_field_map[rel_name]
        existing = existing_doc.get(solr_field, [])
        if isinstance(existing, str):
            existing = [existing]
        if new_pid not in existing:
            all_pids = existing + [new_pid]
        else:
            all_pids = existing
        merged[rel_name] = ','.join(all_pids)
    return merged

def gcp_attach_streams_to_transcripts(api_url, collection, item_api, repo_url, dry_run=False):
    """For each video in the collection, find its stream, then point
    transcripts and translations at the stream."""
    videos = get_videos_in_collection(api_url, collection)
    
    stats = {'videos': 0, 'no_stream': 0, 'transcripts_updated': 0,
             'translations_updated': 0, 'already_done': 0, 'errors': 0}

    for video_doc in videos:
        video_pid = video_doc['pid']
        stats['videos'] += 1
        print(f"\n--- Video: {video_pid} ---")

        # Find the stream for this video
        stream_doc = get_stream_for_video(api_url, video_pid)
        if not stream_doc:
            print(f"  No stream found for {video_pid}, skipping")
            stats['no_stream'] += 1
            continue
        stream_pid = stream_doc['pid']
        print(f"  Stream: {stream_pid}")

        # Process transcripts
        transcripts = get_transcripts_of_video(api_url, video_pid)
        for transcript in transcripts:
            t_pid = transcript['pid']
            if item_already_linked_to_stream(transcript, stream_pid) \
                    and transcript_already_linked_to_stream(transcript, stream_pid):
                print(f"  Transcript {t_pid} already linked to stream, skipping")
                stats['already_done'] += 1
                continue

            print(f"  Transcript {t_pid} -> adding isPartOf + isTranscriptOf -> {stream_pid}")
            if not dry_run:
                try:
                    update_rels_via_xml(item_api, repo_url, t_pid, [
                        (RELSEXT_NS.isPartOf, stream_pid),
                        (BUL_NS.isTranscriptOf, stream_pid),
                    ])
                    stats['transcripts_updated'] += 1
                except Exception as e:
                    print(f"  ERROR updating {t_pid}: {e}")
                    stats['errors'] += 1
            else:
                stats['transcripts_updated'] += 1

        # Process translations
        translations = get_translations_of_video(api_url, video_pid)
        for translation in translations:
            tl_pid = translation['pid']
            if item_already_linked_to_stream(translation, stream_pid):
                print(f"  Translation {tl_pid} already linked to stream, skipping")
                stats['already_done'] += 1
                continue

            print(f"  Translation {tl_pid} -> adding isPartOf -> {stream_pid}")
            if not dry_run:
                try:
                    update_rels_via_xml(item_api, repo_url, tl_pid, [
                        (RELSEXT_NS.isPartOf, stream_pid),
                    ])
                    stats['translations_updated'] += 1
                except Exception as e:
                    print(f"  ERROR updating {tl_pid}: {e}")
                    stats['errors'] += 1
            else:
                stats['translations_updated'] += 1

    # Summary
    print(f"\n{'=== DRY RUN SUMMARY ===' if dry_run else '=== SUMMARY ==='}")
    print(f"Videos processed: {stats['videos']}")
    print(f"Videos without streams: {stats['no_stream']}")
    print(f"Transcripts {'would be ' if dry_run else ''}updated: {stats['transcripts_updated']}")
    print(f"Translations {'would be ' if dry_run else ''}updated: {stats['translations_updated']}")
    print(f"Already done (skipped): {stats['already_done']}")
    if stats['errors']:
        print(f"Errors: {stats['errors']}")

def main():
    load_dotenv()
    api_url = os.environ["SOLR_URL"]
    storage_url = os.environ["BACKEND_STORAGE_BASE_URL"]
    item_api = os.environ["API_URL"]

    parser = argparse.ArgumentParser(
        description="makes streams and adds stream to parent for GCP"
    )

    parser.add_argument('collection',
        type=str,
        help='PID of the collection to operate on (e.g. bdr:12345)'
    )

    parser.add_argument("-q","--queue-stream-jobs",
        action="store_true",
        help="queue stream jobs for GCP collection",
        dest='queue'
    )
    parser.add_argument("-a","--add-stream-to-parents",
        action="store_true",
        help="add stream IDs to parents in GCP collection",
        dest='add'
    )
    parser.add_argument("-t","--attach-streams-to-transcripts",
        action="store_true",
        help="attach stream PIDs to transcripts/translations as parents",
        dest='attach_transcripts'
    )
    parser.add_argument("--dry-run",
        action="store_true",
        help="show what would be done without making changes",
        dest='dry_run'
    )

    args = parser.parse_args()
    collection = args.collection

    selected = sum([args.queue, args.add, args.attach_transcripts])
    if selected != 1:
        print("please select exactly one operation (-q, -a, or -t)")
        parser.print_help()
        return

    if args.queue:
        print("queueing jobs for full gcp collection")
        gcp_make_streams(api_url, collection)
        return
    if args.add:
        print("attaching streams to parents for full gcp collection")
        gcp_attach_streams_to_parents(api_url, collection, item_api)
        return
    if args.attach_transcripts:
        if args.dry_run:
            print("DRY RUN: showing what would be done")
        print("attaching streams to transcripts/translations")
        gcp_attach_streams_to_transcripts(api_url, collection, item_api, storage_url, dry_run=args.dry_run)
        return

if __name__ == "__main__":
    main()