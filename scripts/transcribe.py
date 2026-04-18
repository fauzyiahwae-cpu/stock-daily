#!/usr/bin/env python3
"""
美股财经日报 - 无字幕视频转录脚本
流程：
  1. 直接调 YouTube API 拉今日博主视频列表（绕过 Worker，避免 Cloudflare 拦截）
  2. 从 Worker /api/transcript?list=1 读已转录 ID，跳过已处理视频
  3. 对无字幕视频：supadata 先试，拿不到则 yt-dlp 下载 → Groq Whisper 转录
  4. 把转录文本写入 Cloudflare KV（key = transcript_{videoId}）
"""

import os
import sys
import json
import time
import tempfile
import subprocess
import urllib.request
import urllib.error
import urllib.parse
from datetime import datetime, timezone, timedelta

# ── 配置 ──────────────────────────────────────────────────────
WORKER_URL    = os.environ.get('WORKER_URL', 'https://stock-daily.fauzyiahwae.workers.dev')
GROQ_API_KEY  = os.environ.get('GROQ_API_KEY', '')
WORKER_SECRET = os.environ.get('WORKER_SECRET', '')
YOUTUBE_KEY   = os.environ.get('YOUTUBE_KEY', '')
SUPADATA_KEY  = os.environ.get('SUPADATA_KEY', 'sd_349fd0179c98859968af2d553a630e26')
MAX_VIDEOS    = int(os.environ.get('MAX_VIDEOS_PER_RUN', '5'))

# ── 博主频道列表（channelId → 名称）──────────────────────────
CHANNELS = {
    'UCFQsi7WaF5X41tcuOryDk8w': '视野环球财经',
    'UCFhJ8ZFg9W4kLwFTBBNIjOw': 'NaNa说美股',
    'UCRBrH2qS7yKGMNSmjnj8gcw': '投资TALK君',
    'UCZuj0RPlwgNf51xKKLeEjvw': 'Amy说美股',
    'UC2I5em6UyBpQiO-8ZW0nV3w': '阳光财经',
    'UCjuvy7VvwUGPtTnbpfYY1QQ': 'XTrader猫姐美股',
    'UCemKtyVRsm-TcYqHQEZH5PQ': '股市咖啡屋',
    'UCwyRBuGpaLYnFuohCYyjBeQ': 'Andy Lee',
    'UCo2gxyermsLBSCxFHvJs0Zg': '老李玩钱',
    'UCwq7HSc0E53LRTnsgpC--Og': 'Amy投资学堂',
    'UCH60IM5V3OO2LrNIDueG26Q': '小左美股第一视角',
    'UCveCI6CK6oPtuy24YH9ii9g': '牛顿师兄',
    'UCzEMrrBM8FY_gWIXVexB92A': '财经企鹅姐',
    'UCWgWYZjNo42YLvx6Arb6mcg': '美股Alpha姐',
}

# 北京时间今日日期
def beijing_today():
    bj = datetime.now(timezone(timedelta(hours=8)))
    return bj.strftime('%Y-%m-%d')

# ── HTTP 工具 ──────────────────────────────────────────────────
def http_get(url, headers=None):
    req = urllib.request.Request(url, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        print(f'  GET 失败 {url[:80]}: {e}')
        return None

def http_post(url, body, headers=None):
    data = json.dumps(body).encode()
    h = {'Content-Type': 'application/json'}
    if headers:
        h.update(headers)
    req = urllib.request.Request(url, data=data, headers=h, method='POST')
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        print(f'  POST 失败 {url}: {e}')
        return None

# ── Step 1：直接调 YouTube API 拉今日视频 ─────────────────────
def fetch_video_list():
    print('📋 直接调 YouTube API 拉取今日视频...')
    if not YOUTUBE_KEY:
        print('  ❌ 缺少 YOUTUBE_KEY')
        return []

    # 北京时间今天 00:00 转为 UTC ISO 格式（publishedAfter 过滤）
    bj_now = datetime.now(timezone(timedelta(hours=8)))
    bj_today_start = bj_now.replace(hour=0, minute=0, second=0, microsecond=0)
    published_after = bj_today_start.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

    channels = []
    for channel_id, name in CHANNELS.items():
        # uploads playlist = UC... → UU...
        playlist_id = 'UU' + channel_id[2:]
        url = (
            'https://www.googleapis.com/youtube/v3/playlistItems'
            f'?part=snippet&playlistId={playlist_id}'
            f'&maxResults=3&key={YOUTUBE_KEY}'
        )
        data = http_get(url)
        if not data:
            print(f'  ⚠️ {name} 获取失败')
            channels.append({'channelId': channel_id, 'name': name, 'videos': []})
            continue

        items = data.get('items', [])
        videos = []
        for item in items:
            snippet = item.get('snippet', {})
            vid_id = snippet.get('resourceId', {}).get('videoId', '')
            published = snippet.get('publishedAt', '')
            # 只取今日发布的
            if published >= published_after and vid_id:
                videos.append({
                    'videoId': vid_id,
                    'title': snippet.get('title', ''),
                    'publishedAt': published,
                })

        if videos:
            print(f'  ✅ {name}: {len(videos)} 个今日视频')
        channels.append({'channelId': channel_id, 'name': name, 'videos': videos})

    total = sum(len(c['videos']) for c in channels)
    print(f'  共找到 {total} 个今日视频')
    return channels

# ── Step 2：已转录 ID（本次运行内存去重，不依赖 Worker）────────
def fetch_transcribed_ids():
    # Cloudflare 会拦截 Actions IP，改为内存去重（同一次运行不重复处理）
    return set()

# ── Step 3a：supadata 试抓字幕 ────────────────────────────────
def try_supadata(video_id):
    url = f'https://supadata.ai/api/youtube/transcript?videoId={video_id}&text=true'
    req = urllib.request.Request(url, headers={'x-api-key': SUPADATA_KEY})
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            data = json.loads(r.read().decode())
            content = data.get('content', '')
            if content and len(content) > 100:
                print(f'  ✅ supadata 字幕成功 ({len(content)} 字符)')
                return content
    except Exception as e:
        pass
    return None

# ── Step 3b：yt-dlp + Webshare 住宅代理下载音频 ──────────────
# Webshare 免费代理列表（自动轮换）
PROXIES = [
    'http://lgpmhapx:c0ugt815uf3f@31.59.20.176:6754',
    'http://lgpmhapx:c0ugt815uf3f@198.23.239.134:6540',
    'http://lgpmhapx:c0ugt815uf3f@45.38.107.97:6014',
    'http://lgpmhapx:c0ugt815uf3f@107.172.163.27:6543',
    'http://lgpmhapx:c0ugt815uf3f@198.105.121.200:6462',
    'http://lgpmhapx:c0ugt815uf3f@216.10.27.159:6837',
    'http://lgpmhapx:c0ugt815uf3f@142.111.67.146:5611',
    'http://lgpmhapx:c0ugt815uf3f@191.96.254.138:6185',
    'http://lgpmhapx:c0ugt815uf3f@31.58.9.4:6077',
    'http://lgpmhapx:c0ugt815uf3f@23.26.71.145:5628',
]

def download_audio(video_id, tmpdir):
    import random
    yt_url = f'https://www.youtube.com/watch?v={video_id}'
    out_path = os.path.join(tmpdir, f'{video_id}.mp3')

    # 随机选一个代理，失败则换下一个
    proxies = random.sample(PROXIES, len(PROXIES))
    for proxy in proxies[:3]:  # 最多试3个
        ip_port = proxy.split('@')[1]
        print(f'  🌐 尝试代理 {ip_port}...')
        cmd = [
            'yt-dlp',
            '--proxy', proxy,
            '-x', '--audio-format', 'mp3',
            '--audio-quality', '5',
            '--max-filesize', '50m',
            '--no-playlist',
            '--quiet',
            '-o', out_path,
            yt_url,
        ]
        try:
            result = subprocess.run(cmd, timeout=180, capture_output=True, text=True)
            if result.returncode == 0 and os.path.exists(out_path):
                size_mb = os.path.getsize(out_path) / 1024 / 1024
                print(f'  ✅ 音频下载成功 ({size_mb:.1f} MB)')
                return out_path
            else:
                err = result.stderr[:200]
                print(f'  ❌ 失败: {err}')
        except subprocess.TimeoutExpired:
            print(f'  ❌ 超时')
        # 换下一个代理前稍等
        time.sleep(2)

    print(f'  ❌ 所有代理均失败')
    return None

# ── Step 4：Groq Whisper 转录 ─────────────────────────────────
def transcribe_groq(audio_path):
    if not GROQ_API_KEY:
        print('  ❌ GROQ_API_KEY 未设置')
        return None

    import groq
    client = groq.Groq(api_key=GROQ_API_KEY)

    file_size = os.path.getsize(audio_path) / 1024 / 1024
    print(f'  🎙️ Groq Whisper 转录中 ({file_size:.1f} MB)...')

    try:
        with open(audio_path, 'rb') as f:
            result = client.audio.transcriptions.create(
                file=(os.path.basename(audio_path), f),
                model='whisper-large-v3',
                language='zh',           # 中文优先
                response_format='text',
                temperature=0.0,
            )
        text = result if isinstance(result, str) else result.text
        print(f'  ✅ 转录完成 ({len(text)} 字符)')
        return text
    except Exception as e:
        print(f'  ❌ Groq 转录失败: {e}')
        return None

# ── Step 5：写入 KV ───────────────────────────────────────────
def save_transcript(video_id, channel_name, transcript):
    body = {
        'videoId': video_id,
        'transcript': transcript,
    }
    headers = {}
    if WORKER_SECRET:
        headers['X-Worker-Secret'] = WORKER_SECRET

    result = http_post(f'{WORKER_URL}/api/transcript', body, headers)
    if result and result.get('saved'):
        print(f'  ✅ 转录已写入 KV (transcript_{video_id})')
        return True
    else:
        print(f'  ❌ KV 写入失败: {result}')
        return False

# ── 主流程 ────────────────────────────────────────────────────
def main():
    print(f'\n{"="*50}')
    print(f'🎬 美股财经日报 转录任务')
    print(f'📅 北京时间: {beijing_today()}')
    print(f'{"="*50}\n')

    if not GROQ_API_KEY:
        print('❌ 缺少 GROQ_API_KEY，退出')
        sys.exit(1)

    # 拉视频列表
    channels = fetch_video_list()
    if not channels:
        print('⚠️ 今日暂无视频，退出')
        return

    # 读已转录列表去重
    cached_ids = fetch_transcribed_ids()

    # 筛选：有今日视频、且未缓存的
    todo = []
    for ch in channels:
        videos = ch.get('videos', [])
        if not videos:
            continue
        vid = videos[0]['videoId']
        name = ch.get('name', '?')
        if vid in cached_ids:
            print(f'  ⏭️  {name} 已转录，跳过')
            continue
        todo.append({'name': name, 'videoId': vid, 'title': videos[0].get('title', '')})

    if not todo:
        print('\n✅ 所有频道均已处理，无需转录')
        return

    print(f'\n📝 需要处理 {len(todo)} 个频道\n')

    success = 0
    with tempfile.TemporaryDirectory() as tmpdir:
        for ch in todo[:MAX_VIDEOS]:
            name = ch['name']
            vid  = ch['videoId']
            print(f'\n▶ {name} ({vid})')

            # 先试 supadata
            transcript = try_supadata(vid)

            # supadata 失败 → yt-dlp + Groq
            if not transcript:
                print('  supadata 无字幕，尝试 yt-dlp + Groq Whisper...')
                audio_path = download_audio(vid, tmpdir)
                if audio_path:
                    transcript = transcribe_groq(audio_path)
                    # 清理音频文件节省空间
                    try:
                        os.remove(audio_path)
                    except:
                        pass

            if not transcript:
                print(f'  ⚠️ {name} 无法获取文本，跳过')
                continue

            # 写入 KV
            if save_transcript(vid, name, transcript):
                success += 1

            # 每个视频之间稍作间隔，避免频繁请求
            time.sleep(2)

    print(f'\n{"="*50}')
    print(f'✅ 完成！成功处理 {success}/{len(todo)} 个频道')
    print(f'{"="*50}\n')

if __name__ == '__main__':
    main()
