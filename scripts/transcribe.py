#!/usr/bin/env python3
"""
美股财经日报 - 无字幕视频转录脚本
流程：
  1. 从 Worker /api/analysis 拉今日博主视频列表
  2. 从 Worker /api/cache 读已有缓存，跳过已分析的博主
  3. 对无字幕的博主视频：supadata 先试一次，拿不到则 yt-dlp 下载音频
  4. Groq Whisper 转录
  5. 把转录文本写入 Cloudflare KV（key = transcript_{videoId}）
"""

import os
import sys
import json
import time
import tempfile
import subprocess
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta

# ── 配置 ──────────────────────────────────────────────────────
WORKER_URL    = os.environ.get('WORKER_URL', 'https://stock-daily.fauzyiahwae.workers.dev')
GROQ_API_KEY  = os.environ.get('GROQ_API_KEY', '')
WORKER_SECRET = os.environ.get('WORKER_SECRET', '')  # 可选，用于 Worker 写入鉴权

SUPADATA_KEY  = 'sd_349fd0179c98859968af2d553a630e26'

# 北京时间今日日期（用于 KV key）
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
        print(f'  GET 失败 {url}: {e}')
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

# ── Step 1：拉今日视频列表 ──────────────────────────────────────
def fetch_video_list():
    print('📋 拉取今日博主视频列表...')
    data = http_get(f'{WORKER_URL}/api/analysis')
    if not data:
        print('  ❌ 无法获取视频列表')
        return []
    channels = data.get('channels', [])
    print(f'  获取到 {len(channels)} 个频道')
    return channels

# ── Step 2：读已有转录，找出需要处理的 ───────────────────────
def fetch_transcribed_ids():
    # 从 /api/transcript?list=1 拿已转录的 videoId 列表
    data = http_get(f'{WORKER_URL}/api/transcript?list=1')
    if data and isinstance(data.get('videoIds'), list):
        ids = set(data['videoIds'])
        print(f'  已转录 {len(ids)} 个视频')
        return ids
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

# ── Step 3b：yt-dlp 下载音频 ──────────────────────────────────
def download_audio(video_id, tmpdir):
    url = f'https://www.youtube.com/watch?v={video_id}'
    out_path = os.path.join(tmpdir, f'{video_id}.mp3')
    cmd = [
        'yt-dlp',
        '-x', '--audio-format', 'mp3',
        '--audio-quality', '5',          # 中等质量，减小文件体积
        '--max-filesize', '30m',          # 最大 30MB（约 30 分钟音频）
        '-o', out_path,
        '--no-playlist',
        '--quiet',
        url
    ]
    try:
        result = subprocess.run(cmd, timeout=120, capture_output=True, text=True)
        if result.returncode == 0 and os.path.exists(out_path):
            size_mb = os.path.getsize(out_path) / 1024 / 1024
            print(f'  ✅ 音频下载成功 ({size_mb:.1f} MB)')
            return out_path
        else:
            print(f'  ❌ yt-dlp 失败: {result.stderr[:200]}')
            return None
    except subprocess.TimeoutExpired:
        print('  ❌ yt-dlp 超时')
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

    # 筛选：有视频ID、且未缓存的频道
    todo = []
    for ch in channels:
        vid = ch.get('latestVideoId') or ch.get('videoId')
        if not vid:
            continue
        if vid in cached_ids:
            print(f'  ⏭️  {ch.get("name","?")} 已缓存，跳过')
            continue
        todo.append(ch)

    if not todo:
        print('\n✅ 所有频道均已处理，无需转录')
        return

    print(f'\n📝 需要处理 {len(todo)} 个频道\n')

    success = 0
    with tempfile.TemporaryDirectory() as tmpdir:
        for ch in todo:
            name = ch.get('name', '未知')
            vid  = ch.get('latestVideoId') or ch.get('videoId')
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
