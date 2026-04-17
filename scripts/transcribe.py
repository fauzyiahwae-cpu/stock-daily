#!/usr/bin/env python3
"""
美股财经日报 - 无字幕视频转录脚本
流程：读KV博主列表 → 拉今日视频 → 过滤已有字幕 → yt-dlp下载 → Whisper转录 → 写入KV
"""

import os
import re
import sys
import json
import time
import subprocess
import tempfile
import urllib.request
import urllib.parse
import urllib.error
from datetime import datetime, timezone, timedelta

# ── 配置 ──────────────────────────────────────────────────────
WORKER_URL          = os.environ['WORKER_URL']
YOUTUBE_KEY         = os.environ['YOUTUBE_KEY']
OPENAI_KEY          = os.environ.get('GROQ_KEY') or os.environ['OPENAI_KEY']  # 优先用 GROQ_KEY
WHISPER_URL         = os.environ.get('WHISPER_URL', 'https://api.groq.com/openai/v1/audio/transcriptions')  # Groq 免费层，兼容 OpenAI 接口
SUPADATA_KEY        = os.environ.get('SUPADATA_KEY', '')
YOUTUBE_COOKIES_B64 = os.environ.get('YOUTUBE_COOKIES', '')  # base64 编码的 cookies.txt 内容
MAX_DURATION_MIN    = int(os.environ.get('MAX_DURATION_MIN', '30'))
MAX_VIDEOS_PER_RUN  = int(os.environ.get('MAX_VIDEOS_PER_RUN', '3'))
WHISPER_SIZE_LIMIT_MB = 24  # Whisper API 硬限制 25MB，留1MB余量

# ── HTTP 工具 ──────────────────────────────────────────────────
def http_get(url, headers=None):
    req = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode())

def http_post(url, data, headers=None):
    body = json.dumps(data).encode()
    req = urllib.request.Request(url, data=body, headers={
        'Content-Type': 'application/json',
        **(headers or {})
    })
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read().decode())

# ── 获取博主列表 ────────────────────────────────────────────────
def get_bloggers():
    try:
        url = f'{WORKER_URL}/api/config'
        print(f'[config] 请求: {url}')
        req = urllib.request.Request(url, headers={'User-Agent': 'stock-daily-actions/1.0'})
        with urllib.request.urlopen(req, timeout=30) as r:
            d = json.loads(r.read().decode())
        bloggers = d.get('config', {}).get('bloggers', [])
        if not bloggers:
            bloggers = d.get('bloggers', [])
        print(f'[config] 博主数量: {len(bloggers)}')
        return bloggers
    except urllib.error.HTTPError as e:
        body = e.read().decode()[:500]
        print(f'[config] HTTP {e.code} 失败: {body}')
        sys.exit(1)
    except Exception as e:
        print(f'[config] 失败: {e}')
        sys.exit(1)

# ── 获取已存 KV 的转录 videoId（避免重复转录）──────────────────
def get_transcribed_ids():
    """
    不依赖 cache 里的 transcribed 标记（该字段不一定存在），
    改为直接查询 KV transcript_* 前缀。
    Worker /api/transcript?list=1 返回已有转录的 videoId 列表。
    """
    try:
        d = http_get(f'{WORKER_URL}/api/transcript?list=1')
        ids = set(d.get('videoIds', []))
        print(f'[transcript] 已有转录: {len(ids)} 个')
        return ids
    except Exception as e:
        print(f'[transcript] 读取已转录列表失败（继续）: {e}')
        return set()  # 失败时返回空集，最多重复转录，不阻断流程

# ── 获取频道今日视频 ────────────────────────────────────────────
def get_today_videos(channel_id):
    # 获取 uploads playlist ID
    try:
        url = (f'https://www.googleapis.com/youtube/v3/channels'
               f'?part=contentDetails&id={channel_id}&key={YOUTUBE_KEY}')
        d = http_get(url)
        items = d.get('items', [])
        if not items:
            print(f'  [youtube] 频道不存在或无权限: {channel_id}')
            return []
        playlist_id = items[0]['contentDetails']['relatedPlaylists']['uploads']
    except Exception as e:
        print(f'  [youtube] 获取playlist失败: {e}')
        return []

    # 拉最新视频（maxResults=10 防止高频博主今日多发）
    try:
        url = (f'https://www.googleapis.com/youtube/v3/playlistItems'
               f'?part=snippet&playlistId={playlist_id}&maxResults=10&key={YOUTUBE_KEY}')
        d = http_get(url)
        items = d.get('items', [])
    except Exception as e:
        print(f'  [youtube] 拉视频列表失败: {e}')
        return []

    # 过滤今日（北京时间）视频
    now_beijing = datetime.now(timezone(timedelta(hours=8)))
    today_str = now_beijing.strftime('%Y-%m-%d')
    videos = []
    for item in items:
        snippet = item.get('snippet', {})
        published_raw = snippet.get('publishedAt', '')
        # publishedAt 是 UTC，需转北京时间判断
        try:
            pub_utc = datetime.strptime(published_raw, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)
            pub_beijing = pub_utc.astimezone(timezone(timedelta(hours=8)))
            pub_date = pub_beijing.strftime('%Y-%m-%d')
        except:
            pub_date = published_raw[:10]  # fallback

        if pub_date != today_str:
            continue

        title = snippet.get('title', '')
        # 过滤 Shorts：标题含关键词 或 标题字数过少
        if '#shorts' in title.lower() or '#short' in title.lower():
            continue
        if len(title.strip()) < 5:
            continue

        video_id = snippet.get('resourceId', {}).get('videoId', '')
        if not video_id:
            continue

        videos.append({'videoId': video_id, 'title': title})

    return videos

# ── 检查视频是否有字幕（YouTube captions API，免费不消耗 supadata）──
def has_caption(video_id):
    """
    用 YouTube Data API 的 captions 端点检查是否存在字幕轨道。
    免费，不消耗 supadata 配额。
    返回 True = 有字幕轨道，跳过转录
    返回 False = 无字幕或查询失败（继续转录流程）
    注意：有字幕轨道不代表 supadata 一定能拿到（私有字幕/disabled），
    但对我们的博主群体来说基本等价。
    """
    try:
        url = (f'https://www.googleapis.com/youtube/v3/captions'
               f'?part=snippet&videoId={video_id}&key={YOUTUBE_KEY}')
        d = http_get(url)
        items = d.get('items', [])
        if not items:
            return False
        # 过滤自动生成的字幕（asr），只认手动上传的
        # 实际上 asr 字幕 supadata 也能拿到，所以两种都算
        has = any(
            item.get('snippet', {}).get('trackKind') in ('standard', 'asr')
            for item in items
        )
        print(f'  [captions] 字幕轨道: {len(items)} 个, has={has}')
        return has
    except urllib.error.HTTPError as e:
        if e.code == 403:
            # captions API 对部分频道禁用，保守返回 False 继续转录
            print(f'  [captions] 403 无权限查字幕，继续转录')
            return False
        print(f'  [captions] HTTP {e.code}，继续转录')
        return False
    except Exception as e:
        print(f'  [captions] 查询异常({e})，继续转录')
        return False

# ── 获取视频时长 ────────────────────────────────────────────────
def get_video_duration(video_id):
    """
    返回秒数。失败返回 -1（区别于 0），让调用方决定是否跳过。
    """
    try:
        url = (f'https://www.googleapis.com/youtube/v3/videos'
               f'?part=contentDetails&id={video_id}&key={YOUTUBE_KEY}')
        d = http_get(url)
        items = d.get('items', [])
        if not items:
            print(f'  [duration] 视频不存在: {video_id}')
            return -1
        duration_str = items[0]['contentDetails']['duration']
        m = re.match(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?', duration_str)
        if not m:
            print(f'  [duration] 无法解析时长: {duration_str}')
            return -1
        h  = int(m.group(1) or 0)
        mn = int(m.group(2) or 0)
        s  = int(m.group(3) or 0)
        return h * 3600 + mn * 60 + s
    except Exception as e:
        print(f'  [duration] 获取失败: {e}')
        return -1

# ── yt-dlp 下载音频 ─────────────────────────────────────────────
def download_audio(video_id, output_dir, cookies_path=None):
    """
    下载为 mp3，audio-quality 5（128kbps，够 Whisper 识别，文件小）。
    yt-dlp 输出文件名可能带后缀如 .webm.mp3，用 glob 查找实际文件。
    """
    url = f'https://www.youtube.com/watch?v={video_id}'
    output_tmpl = os.path.join(output_dir, f'{video_id}.%(ext)s')
    cmd = [
        'yt-dlp',
        '--extract-audio',
        '--audio-format', 'mp3',
        '--audio-quality', '5',
        '--no-playlist',
        '--output', output_tmpl,
        '--quiet',
        '--no-warnings',
        '--limit-rate', '2M',
        url
    ]
    # 注入 cookies（避免 YouTube 反爬）
    if cookies_path and os.path.exists(cookies_path):
        cmd.insert(1, cookies_path)
        cmd.insert(1, '--cookies')
    try:
        result = subprocess.run(cmd, timeout=180, capture_output=True, text=True)
        if result.returncode != 0:
            print(f'  [yt-dlp] 失败(code={result.returncode}): {result.stderr[:300]}')
            return None

        # 查找实际输出文件（扩展名可能变化）
        import glob
        files = glob.glob(os.path.join(output_dir, f'{video_id}.*'))
        if not files:
            print(f'  [yt-dlp] 下载完成但找不到文件')
            return None

        audio_path = files[0]
        size_mb = os.path.getsize(audio_path) / 1024 / 1024
        print(f'  [yt-dlp] 下载成功 {size_mb:.1f}MB: {os.path.basename(audio_path)}')
        return audio_path

    except subprocess.TimeoutExpired:
        print(f'  [yt-dlp] 下载超时(180s)')
        return None
    except Exception as e:
        print(f'  [yt-dlp] 异常: {e}')
        return None

# ── Whisper 转录 ────────────────────────────────────────────────
def transcribe_audio(audio_path):
    file_size_mb = os.path.getsize(audio_path) / 1024 / 1024
    print(f'  [whisper] 开始转录 {file_size_mb:.1f}MB...')

    if file_size_mb > WHISPER_SIZE_LIMIT_MB:
        print(f'  [whisper] 文件超过{WHISPER_SIZE_LIMIT_MB}MB限制，跳过')
        return None

    if file_size_mb < 0.01:
        print(f'  [whisper] 文件过小，可能下载不完整，跳过')
        return None

    try:
        boundary = 'WhisperFormBoundary'
        with open(audio_path, 'rb') as f:
            audio_data = f.read()

        # 构造 multipart/form-data
        parts = []
        for name, value in [('model', 'whisper-large-v3-turbo'), ('language', 'zh')]:
            parts.append(
                f'--{boundary}\r\n'
                f'Content-Disposition: form-data; name="{name}"\r\n\r\n'
                f'{value}\r\n'.encode()
            )
        parts.append(
            f'--{boundary}\r\n'
            f'Content-Disposition: form-data; name="file"; filename="audio.mp3"\r\n'
            f'Content-Type: audio/mpeg\r\n\r\n'.encode()
            + audio_data
            + b'\r\n'
        )
        parts.append(f'--{boundary}--\r\n'.encode())
        body = b''.join(parts)

        req = urllib.request.Request(
            WHISPER_URL,
            data=body,
            headers={
                'Authorization': f'Bearer {OPENAI_KEY}',
                'Content-Type': f'multipart/form-data; boundary={boundary}',
            }
        )
        with urllib.request.urlopen(req, timeout=300) as r:
            d = json.loads(r.read().decode())
            text = d.get('text', '').strip()
            if not text:
                print(f'  [whisper] 返回空文本')
                return None
            print(f'  [whisper] 转录成功，字符数: {len(text)}')
            return text

    except urllib.error.HTTPError as e:
        body = e.read().decode()[:200]
        print(f'  [whisper] HTTP {e.code}: {body}')
        return None
    except Exception as e:
        print(f'  [whisper] 失败: {e}')
        return None

# ── 写转录结果到 KV ─────────────────────────────────────────────
def save_transcript(video_id, transcript):
    try:
        http_post(
            f'{WORKER_URL}/api/transcript',
            {'videoId': video_id, 'transcript': transcript}
        )
        print(f'  [kv] 保存成功: {video_id}')
        return True
    except Exception as e:
        print(f'  [kv] 保存失败: {e}')
        return False  # 保存失败不阻断，下次会重试

# ── 主流程 ──────────────────────────────────────────────────────
def main():
    print(f'=== 转录任务开始 {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} ===')
    print(f'配置: MAX_DURATION={MAX_DURATION_MIN}min, MAX_VIDEOS={MAX_VIDEOS_PER_RUN}')

    # 写入 YouTube cookies 文件（供 yt-dlp 使用）
    cookies_path = None
    if YOUTUBE_COOKIES_B64:
        import base64
        cookies_path = os.path.join(tempfile.gettempdir(), 'youtube_cookies.txt')
        with open(cookies_path, 'w') as f:
            f.write(YOUTUBE_COOKIES_B64)
        print(f'[cookies] 已写入: {cookies_path}')
    else:
        print('[cookies] 未配置 YOUTUBE_COOKIES，yt-dlp 可能被反爬拦截')

    bloggers = get_bloggers()
    transcribed_ids = get_transcribed_ids()
    processed = 0

    with tempfile.TemporaryDirectory() as tmp_dir:
        for blogger in bloggers:
            if processed >= MAX_VIDEOS_PER_RUN:
                print(f'\n已达到单次上限({MAX_VIDEOS_PER_RUN}个)，停止')
                break

            channel_id = blogger.get('channelId', '').strip()
            name = blogger.get('name', channel_id)
            if not channel_id:
                print(f'  [skip] 博主无 channelId: {name}')
                continue

            print(f'\n--- {name} ({channel_id}) ---')
            videos = get_today_videos(channel_id)
            if not videos:
                print('  今日无视频')
                continue

            print(f'  今日视频: {len(videos)} 个')

            for video in videos:
                if processed >= MAX_VIDEOS_PER_RUN:
                    break

                video_id = video['videoId']
                title = video['title']
                print(f'\n  [{video_id}] {title[:50]}')

                # 已转录过（KV 有记录）
                if video_id in transcribed_ids:
                    print(f'  KV已有转录，跳过')
                    continue

                # 检查字幕（supadata）
                print(f'  检查字幕...')
                if has_caption(video_id):
                    print(f'  有字幕，跳过（前端 supadata 处理）')
                    continue

                # 获取时长
                duration_sec = get_video_duration(video_id)
                if duration_sec == -1:
                    print(f'  时长获取失败，跳过')
                    continue
                duration_min = duration_sec / 60
                print(f'  时长: {duration_min:.1f} 分钟')

                if duration_min < 1:
                    print(f'  时长 < 1分钟，可能是 Shorts，跳过')
                    continue
                if duration_min > MAX_DURATION_MIN:
                    print(f'  超过 {MAX_DURATION_MIN} 分钟上限，跳过')
                    continue

                # 下载音频
                print(f'  下载音频...')
                audio_path = download_audio(video_id, tmp_dir, cookies_path=cookies_path)
                if not audio_path:
                    print(f'  下载失败，跳过')
                    continue

                # Whisper 转录
                transcript = transcribe_audio(audio_path)

                # 清理音频（无论成功与否）
                try:
                    os.remove(audio_path)
                except:
                    pass

                if not transcript:
                    print(f'  转录失败，跳过')
                    continue
                if len(transcript) < 100:
                    print(f'  转录内容过少({len(transcript)}字)，跳过')
                    continue

                # 保存到 KV
                if save_transcript(video_id, transcript):
                    transcribed_ids.add(video_id)
                    processed += 1

                time.sleep(2)  # 避免频繁请求

    print(f'\n=== 完成，共转录 {processed} 个视频 ===')

if __name__ == '__main__':
    main()
