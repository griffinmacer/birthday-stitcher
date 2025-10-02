import os, sys, json, csv, shlex, subprocess
from datetime import datetime
from urllib.parse import unquote
from pathlib import Path

import boto3
from botocore.config import Config
from tqdm import tqdm

# ---------- READ CONFIG FROM ENV (set via GitHub Actions) ----------
ACCOUNT_ID = os.environ["R2_ACCOUNT_ID"]
ACCESS_KEY_ID = os.environ["R2_ACCESS_KEY_ID"]
SECRET_ACCESS_KEY = os.environ["R2_SECRET_ACCESS_KEY"]
BUCKET = os.environ["R2_BUCKET"]

# Inputs provided at workflow run time:
PREFIX = os.getenv("R2_PREFIX", "").strip()         # e.g., "uploads/sister-40/"
OUTPUT_KEY = os.getenv("OUTPUT_KEY", "").strip()    # e.g., "finals/sister-40/Happy40.mp4"
SORT_MODE = os.getenv("SORT_MODE", "last_modified") # "manifest" | "name" | "last_modified"
TITLE_TEXT = os.getenv("TITLE_TEXT", "").strip()    # non-empty => add an intro card
LABEL_CLIPS = os.getenv("LABEL_CLIPS", "false").lower() == "true"  # add name label first 3s
GEN_PRESIGNED = os.getenv("GENERATE_PRESIGNED_URL", "false").lower() == "true"

# ---------- R2 S3-COMPATIBLE CLIENT ----------
ENDPOINT_URL = f"https://{ACCOUNT_ID}.r2.cloudflarestorage.com"
s3 = boto3.client(
    "s3",
    endpoint_url=ENDPOINT_URL,
    aws_access_key_id=ACCESS_KEY_ID,
    aws_secret_access_key=SECRET_ACCESS_KEY,
    region_name="auto",
    config=Config(read_timeout=300, retries={"max_attempts": 10, "mode": "standard"}),
)

# ---------- UTILS ----------
def run(cmd):
    print(">>>", " ".join(shlex.quote(c) for c in cmd), flush=True)
    subprocess.run(cmd, check=True)

def ffprobe_json(path):
    cmd = [
        "ffprobe", "-v", "error",
        "-print_format", "json",
        "-show_streams", "-show_format",
        str(path)
    ]
    out = subprocess.check_output(cmd, text=True)
    return json.loads(out)

def rotation_from_meta(meta):
    rot = 0
    streams = meta.get("streams", [])
    v = next((s for s in streams if s.get("codec_type") == "video"), None)
    if not v:
        return 0
    tags = v.get("tags", {})
    if "rotate" in tags:
        try:
            return int(tags["rotate"]) % 360
        except Exception:
            pass
    for sd in v.get("side_data_list", []):
        if "rotation" in sd:
            try:
                return int(sd["rotation"]) % 360
            except Exception:
                pass
    return 0

def ff_esc(text: str) -> str:
    # Escape for ffmpeg drawtext (simple version)
    return text.replace("\\", "\\\\").replace(":", "\\:").replace("'", r"\'").replace(",", "\\,")

def make_label_from_filename(name):
    base = Path(name).name
    base = os.path.splitext(base)[0]
    base = base.replace("_", " ").replace("-", " ")
    parts = base.split()
    if len(parts) >= 2 and parts[0].isdigit():
        base = " ".join(parts[1:])
    return base.title().strip()

def list_video_keys(prefix):
    paginator = s3.get_paginator("list_objects_v2")
    keep_ext = (".mp4",".mov",".m4v",".webm",".mkv",".avi",".3gp")
    items = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.lower().endswith(keep_ext):
                items.append({"Key": key, "LastModified": obj["LastModified"], "Size": obj["Size"]})
    return items

def read_manifest(prefix):
    # Looks for "<prefix>manifest.csv"
    man_key = (prefix.rstrip("/") + "/manifest.csv") if not prefix.endswith("manifest.csv") else prefix
    try:
        resp = s3.get_object(Bucket=BUCKET, Key=man_key)
        rows = resp["Body"].read().decode("utf-8").splitlines()
        r = csv.DictReader(rows)
        order = []
        for row in r:
            k = row.get("key") or row.get("Key")
            disp = row.get("display_name") or row.get("name") or ""
            if k:
                order.append({"Key": k, "Display": disp})
        return order
    except s3.exceptions.NoSuchKey:
        return None
    except Exception as e:
        print("Manifest read error:", e)
        return None

def ensure_dirs():
    Path("downloads").mkdir(exist_ok=True)
    Path("clips").mkdir(exist_ok=True)

def download_to(path, key):
    path.parent.mkdir(parents=True, exist_ok=True)
    s3.download_file(BUCKET, key, str(path))

def transcode_to_uniform(infile, outfile, label_text=""):
    """
    Re-encode each clip to the same spec so concat is reliable:
      - 1920x1080 canvas (keep aspect: scale & pad), yuv420p
      - 30 fps
      - H.264 (libx264) CRF 21, veryfast
      - AAC 192k, 48kHz, stereo
      - EBU R128 loudness normalization
      - Optional drawtext label first 3s
      - Honor rotation metadata
    """
    meta = ffprobe_json(infile)
    rot = rotation_from_meta(meta)

    vf_parts = []
    if rot == 90:
        vf_parts.append("transpose=1")
    elif rot == 270:
        vf_parts.append("transpose=2")
    elif rot == 180:
        vf_parts.append("hflip,vflip")

    vf_parts.append("scale='min(1920,iw)':'-2':force_original_aspect_ratio=decrease")
    vf_parts.append("pad=1920:1080:(ow-iw)/2:(oh-ih)/2:color=black")
    vf_parts.append("setsar=1")
    vf_parts.append("format=yuv420p")

    if label_text:
        label = ff_esc(label_text)
        font = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
        draw = (
            f"drawtext=fontfile='{font}':text='{label}':"
            f"x=(w-text_w)/2:y=h-120:fontsize=48:fontcolor=white:"
            f"box=1:boxcolor=black@0.45:boxborderw=20:enable='lt(t,3)'"
        )
        vf_parts.append(draw)

    vf = ",".join(vf_parts)

    cmd = [
        "ffmpeg","-y","-nostdin","-i", str(infile),
        "-vf", vf,
        "-r","30",
        "-c:v","libx264","-preset","veryfast","-crf","21",
        "-c:a","aac","-b:a","192k","-ar","48000","-ac","2",
        "-af","loudnorm=I=-16:TP=-1.5:LRA=11",
        "-movflags","+faststart",
        str(outfile)
    ]
    run(cmd)

def make_title_card(outfile, text):
    font = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
    safe = ff_esc(text)
    vf = (
        f"drawtext=fontfile='{font}':"
        f"text='{safe}':x=(w-text_w)/2:y=(h-text_h)/2:"
        f"fontsize=84:fontcolor=white:box=1:boxcolor=black@0.35:boxborderw=35"
    )
    cmd = [
        "ffmpeg","-y","-f","lavfi","-i","color=c=black:s=1920x1080:d=3",
        "-vf", vf,
        "-r","30","-c:v","libx264","-preset","veryfast","-crf","21",
        "-c:a","aac","-b:a","128k","-ar","48000","-ac","2",
        "-movflags","+faststart",
        str(outfile)
    ]
    run(cmd)

def write_concat_list(filelist_path, parts):
    with open(filelist_path, "w", encoding="utf-8") as f:
        for p in parts:
            f.write(f"file {shlex.quote(str(p))}\n")

def concat_files(filelist_path, output_path):
    cmd = [
        "ffmpeg","-y","-f","concat","-safe","0",
        "-i", str(filelist_path),
        "-c","copy","-movflags","+faststart",
        str(output_path)
    ]
    run(cmd)

def normalize_prefix(pfx: str, bucket: str) -> str:
    # Accepts "uploads/", "birthday-videos/uploads/", or "s3://birthday-videos/uploads/"
    if not pfx:
        return ""
    variants = [f"s3://{bucket}/", f"{bucket}/", f"/{bucket}/"]
    for v in variants:
        if pfx.startswith(v):
            pfx = pfx[len(v):]
            break
    pfx = pfx.lstrip("/")
    if pfx and not pfx.endswith("/"):
        pfx += "/"
    return pfx

def main():
    global PREFIX, OUTPUT_KEY
    PREFIX = normalize_prefix(PREFIX, BUCKET)
    if not PREFIX:
        print("R2_PREFIX is required. Example: uploads/ or uploads/sister-40/")
        sys.exit(1)

    Path("downloads").mkdir(exist_ok=True)
    Path("clips").mkdir(exist_ok=True)

    manifest = read_manifest(PREFIX) if SORT_MODE == "manifest" else None

    if manifest:
        ordered = [{"Key": row["Key"], "Display": row.get("Display","")} for row in manifest]
        print(f"Using manifest.csv with {len(ordered)} entries")
    else:
        objects = list_video_keys(PREFIX)
        if not objects:
            print("No video objects found under the given prefix.")
            sys.exit(1)
        if SORT_MODE == "name":
            objects.sort(key=lambda x: x["Key"].lower())
        else:  # last_modified
            objects.sort(key=lambda x: x["LastModified"])
        ordered = [{"Key": o["Key"], "Display": ""} for o in objects]
        print(f"Found {len(ordered)} videos")

    parts = []
    with tqdm(total=len(ordered), desc="Transcoding clips") as bar:
        for idx, item in enumerate(ordered, start=1):
            key = item["Key"]
            disp_name = item["Display"] or make_label_from_filename(key) if LABEL_CLIPS else ""
            dl_path = Path("downloads") / f"{idx:03d}-{Path(key).name}"
            out_path = Path("clips") / f"{idx:03d}.mp4"

            print(f"\nDownloading: s3://{BUCKET}/{key}")
            download_to(dl_path, key)

            print(f"Transcoding to uniform spec: {out_path.name}")
            transcode_to_uniform(dl_path, out_path, label_text=disp_name)
            parts.append(out_path)
            bar.update(1)

    final_parts = []
    if TITLE_TEXT:
        title_path = Path("clips") / "000-title.mp4"
        print("\nCreating title card...")
        make_title_card(title_path, TITLE_TEXT)
        final_parts.append(title_path)

    final_parts.extend(parts)

    filelist = Path("filelist.txt")
    write_concat_list(filelist, final_parts)

    tmp_final = Path("final.mp4")
    print("\nConcatenating all parts...")
    concat_files(filelist, tmp_final)

    if not OUTPUT_KEY:
        stamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
        OUTPUT_KEY = f"{PREFIX.rstrip('/')}/final/Happy40-{stamp}.mp4"

    print(f"\nUploading final to: s3://{BUCKET}/{OUTPUT_KEY}")
    s3.upload_file(str(tmp_final), BUCKET, OUTPUT_KEY, ExtraArgs={"ContentType": "video/mp4"})
    print("Upload complete.")

    if GEN_PRESIGNED:
        url = s3.generate_presigned_url(
            ClientMethod="get_object",
            Params={"Bucket": BUCKET, "Key": OUTPUT_KEY},
            ExpiresIn=7*24*3600
        )
        print("\nPresigned download URL (valid ~7 days):")
        print(url)

    print("\nALL DONE ✅")

if __name__ == "__main__":
    main()
