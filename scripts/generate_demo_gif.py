"""
Generate high-fidelity animated demo GIF and screenshot for PocketQueue README.
Renders the Charm Bubbletea TUI, live event stream, JSON Inspector, and DLQ Replay Studio.
"""

import os
import numpy as np
from PIL import Image, ImageDraw, ImageFont
import imageio.v3 as iio

# Canvas configuration
WIDTH = 960
HEIGHT = 580
BG_COLOR = (21, 21, 32)        # #151520 - Charm Dark
TITLEBAR_BG = (30, 30, 46)     # #1e1e2e
BORDER_COLOR = (60, 60, 85)
PURPLE = (125, 86, 244)        # #7D56F4 - PocketQueue Primary
CYAN = (0, 210, 255)           # #00D2FF - Secondary
GREEN = (4, 181, 117)          # #04B575 - Success
AMBER = (255, 174, 0)          # #FFAE00 - Warning
RED = (255, 95, 135)           # #FF5F87 - Danger / DLQ
TEXT_WHITE = (245, 245, 255)
TEXT_MUTED = (120, 120, 150)
CARD_BG = (34, 34, 48)         # #222230
HIGHLIGHT_ROW = (45, 45, 70)

# Load fonts
FONT_DIR = os.path.join(os.environ.get('WINDIR', 'C:\\Windows'), 'Fonts')
font_regular = ImageFont.truetype(os.path.join(FONT_DIR, 'consola.ttf'), 14)
font_bold = ImageFont.truetype(os.path.join(FONT_DIR, 'consolab.ttf'), 14)
font_title = ImageFont.truetype(os.path.join(FONT_DIR, 'consolab.ttf'), 15)
font_badge = ImageFont.truetype(os.path.join(FONT_DIR, 'consolab.ttf'), 13)
font_small = ImageFont.truetype(os.path.join(FONT_DIR, 'consola.ttf'), 13)


def draw_window_frame(draw, title="PocketQueue v0.1.0 — ⚡ localhost:9092 (Kafka) | :6379 (Redis)"):
    # Window background
    draw.rectangle([0, 0, WIDTH, HEIGHT], fill=BG_COLOR)
    # Title bar
    draw.rectangle([0, 0, WIDTH, 36], fill=TITLEBAR_BG)
    draw.line([0, 36, WIDTH, 36], fill=BORDER_COLOR, width=1)
    
    # macOS window traffic light buttons
    draw.ellipse([14, 12, 26, 24], fill=(255, 95, 86))   # Red
    draw.ellipse([34, 12, 46, 24], fill=(255, 189, 46))  # Yellow
    draw.ellipse([54, 12, 66, 24], fill=(39, 201, 63))   # Green
    
    # Window title centered
    bbox = font_small.getbbox(title)
    tw = bbox[2] - bbox[0]
    draw.text(((WIDTH - tw) // 2, 10), title, font=font_small, fill=TEXT_MUTED)
    
    # Outer border
    draw.rectangle([0, 0, WIDTH - 1, HEIGHT - 1], outline=BORDER_COLOR, width=1)


def draw_header_and_tabs(draw, active_tab=1):
    # Top Logo Badge
    badge_text = " ⚡ POCKETQUEUE "
    bbox = font_title.getbbox(badge_text)
    bw = bbox[2] - bbox[0] + 16
    draw.rounded_rectangle([24, 48, 24 + bw, 74], radius=4, fill=PURPLE)
    draw.text((32, 53), badge_text, font=font_title, fill=(255, 255, 255))
    
    subtitle = "Ultra-Lightweight Embedded Broker (<15MB, <25MB RAM, Pure Go)"
    draw.text((24 + bw + 14, 53), subtitle, font=font_bold, fill=TEXT_MUTED)
    
    # Tab 1: Live Stream
    tab1_w = 140
    if active_tab == 1:
        draw.rounded_rectangle([24, 88, 24 + tab1_w, 112], radius=3, fill=PURPLE)
        draw.text((36, 93), "1: Live Stream", font=font_bold, fill=(255, 255, 255))
        draw.text((24 + tab1_w + 20, 93), "2: Topics & DLQ", font=font_regular, fill=TEXT_MUTED)
    else:
        draw.text((36, 93), "1: Live Stream", font=font_regular, fill=TEXT_MUTED)
        draw.rounded_rectangle([180, 88, 180 + tab1_w, 112], radius=3, fill=PURPLE)
        draw.text((192, 93), "2: Topics & DLQ", font=font_bold, fill=(255, 255, 255))
    
    # Underline below tabs
    draw.line([24, 116, WIDTH - 24, 116], fill=BORDER_COLOR, width=1)


def draw_stream_table(draw, events, selected_index=-1):
    # Column headers
    cols = [
        ("TIME", 24, 100),
        ("TOPIC", 124, 210),
        ("PART", 334, 60),
        ("OFFSET", 394, 70),
        ("KEY", 464, 130),
        ("PAYLOAD PREVIEW", 594, 340),
    ]
    
    header_y = 126
    for title, x, _ in cols:
        draw.text((x, header_y), title, font=font_bold, fill=PURPLE)
    
    draw.line([24, 146, WIDTH - 24, 146], fill=(50, 50, 75), width=1)
    
    row_y = 154
    line_h = 26
    
    for i, ev in enumerate(events):
        curr_y = row_y + i * line_h
        
        # Row highlight
        if i == selected_index:
            draw.rounded_rectangle([20, curr_y - 2, WIDTH - 20, curr_y + line_h - 6], radius=4, fill=HIGHLIGHT_ROW)
            # Selection indicator dot
            draw.text((8, curr_y), "▶", font=font_regular, fill=CYAN)
            
        time_str = ev.get("time", "11:25:00")
        topic = ev.get("topic", "orders")
        part = str(ev.get("part", 0))
        offset = str(ev.get("offset", 0))
        key = ev.get("key", "-")
        preview = ev.get("preview", "")
        
        draw.text((cols[0][1], curr_y), time_str, font=font_regular, fill=(180, 180, 200))
        
        # Topic with DLQ alert color
        if "dlq" in topic.lower():
            draw.text((cols[1][1], curr_y), "🚨 " + topic, font=font_bold, fill=RED)
        else:
            draw.text((cols[1][1], curr_y), topic, font=font_regular, fill=CYAN)
            
        draw.text((cols[2][1], curr_y), part, font=font_regular, fill=TEXT_MUTED)
        draw.text((cols[3][1], curr_y), offset, font=font_regular, fill=GREEN)
        draw.text((cols[4][1], curr_y), key, font=font_regular, fill=(220, 220, 240))
        draw.text((cols[5][1], curr_y), preview[:45], font=font_regular, fill=(160, 160, 180))


def draw_topics_table(draw, topics):
    cols = [
        ("TOPIC", 24, 200),
        ("PARTITIONS", 224, 120),
        ("TOTAL MSGS", 344, 120),
        ("EARLIEST", 464, 100),
        ("LATEST", 564, 100),
        ("STATUS", 664, 150),
    ]
    
    header_y = 126
    for title, x, _ in cols:
        draw.text((x, header_y), title, font=font_bold, fill=PURPLE)
    
    draw.line([24, 146, WIDTH - 24, 146], fill=(50, 50, 75), width=1)
    
    row_y = 154
    line_h = 28
    
    for i, t in enumerate(topics):
        curr_y = row_y + i * line_h
        tname = t["name"]
        is_dlq = "dlq" in tname.lower()
        
        if is_dlq:
            draw.text((cols[0][1], curr_y), "🚨 " + tname, font=font_bold, fill=RED)
        else:
            draw.text((cols[0][1], curr_y), tname, font=font_regular, fill=CYAN)
            
        draw.text((cols[1][1], curr_y), str(t["parts"]), font=font_regular, fill=TEXT_MUTED)
        draw.text((cols[2][1], curr_y), str(t["total"]), font=font_regular, fill=TEXT_WHITE)
        draw.text((cols[3][1], curr_y), str(t["earliest"]), font=font_regular, fill=TEXT_MUTED)
        draw.text((cols[4][1], curr_y), str(t["latest"]), font=font_regular, fill=GREEN)
        
        if is_dlq:
            draw.rounded_rectangle([cols[5][1], curr_y - 2, cols[5][1] + 68, curr_y + 18], radius=3, fill=RED)
            draw.text((cols[5][1] + 6, curr_y), "🚨 DLQ", font=font_badge, fill=(255, 255, 255))
        else:
            draw.rounded_rectangle([cols[5][1], curr_y - 2, cols[5][1] + 68, curr_y + 18], radius=3, fill=(15, 90, 60))
            draw.text((cols[5][1] + 10, curr_y), "Active", font=font_badge, fill=GREEN)


def draw_modal(draw, title, lines, is_error=False):
    # Dimming overlay
    overlay = Image.new('RGBA', (WIDTH, HEIGHT), (10, 10, 15, 180))
    # We will composite this onto base image
    
    mx, my, mw, mh = 100, 130, 760, 360
    draw.rounded_rectangle([mx, my, mx + mw, my + mh], radius=8, fill=CARD_BG, outline=PURPLE, width=2)
    
    # Modal Header
    draw.rounded_rectangle([mx + 1, my + 1, mx + mw - 1, my + 38], radius=8, fill=(45, 45, 65))
    draw.text((mx + 20, my + 10), title, font=font_bold, fill=CYAN)
    
    curr_y = my + 50
    for line in lines:
        if isinstance(line, tuple):
            label, val, color = line
            draw.text((mx + 24, curr_y), f"{label:<12} ", font=font_bold, fill=TEXT_MUTED)
            draw.text((mx + 130, curr_y), val, font=font_regular, fill=color)
        else:
            # Code / JSON line
            color = TEXT_WHITE
            if "REJECTED" in line or "IDR_OLD" in line or "error" in line.lower():
                color = RED if is_error else AMBER
            elif "APPROVED" in line or '"IDR"' in line:
                color = GREEN
            elif '"' in line and ':' in line:
                color = (170, 215, 255)
            draw.text((mx + 24, curr_y), line, font=font_regular, fill=color)
        curr_y += 20


def draw_status_bar(draw, status_msg="", key_help="Tab: Switch View • Enter: Inspect • r/e: Replay & Edit • q: Quit"):
    y = HEIGHT - 32
    draw.line([24, y - 8, WIDTH - 24, y - 8], fill=BORDER_COLOR, width=1)
    
    if status_msg:
        if "✅" in status_msg:
            draw.text((24, y), status_msg, font=font_bold, fill=GREEN)
        elif "❌" in status_msg:
            draw.text((24, y), status_msg, font=font_bold, fill=RED)
        else:
            draw.text((24, y), status_msg, font=font_regular, fill=CYAN)
    else:
        draw.text((24, y), "Ready • Kafka (:9092) & Redis Streams (:6379) listening", font=font_regular, fill=TEXT_MUTED)
        
    bbox = font_small.getbbox(key_help)
    kw = bbox[2] - bbox[0]
    draw.text((WIDTH - kw - 24, y), key_help, font=font_small, fill=TEXT_MUTED)


def render_frame_1():
    img = Image.new('RGB', (WIDTH, HEIGHT), BG_COLOR)
    d = ImageDraw.Draw(img)
    draw_window_frame(d)
    draw_header_and_tabs(d, active_tab=1)
    events = [
        {"time": "11:25:01", "topic": "orders", "part": 0, "offset": 0, "key": "ord-101", "preview": '{"sku":"LAPTOP-X","qty":1,"price":1299.99}'},
    ]
    draw_stream_table(d, events)
    draw_status_bar(d, "⚡ Ingress event received via Kafka :9092")
    return img


def render_frame_2():
    img = Image.new('RGB', (WIDTH, HEIGHT), BG_COLOR)
    d = ImageDraw.Draw(img)
    draw_window_frame(d)
    draw_header_and_tabs(d, active_tab=1)
    events = [
        {"time": "11:25:01", "topic": "orders", "part": 0, "offset": 0, "key": "ord-101", "preview": '{"sku":"LAPTOP-X","qty":1,"price":1299.99}'},
        {"time": "11:25:02", "topic": "telemetry", "part": 0, "offset": 0, "key": "dev-alpha", "preview": '{"sensor":"DHT22","temp":24.2,"status":"ok"}'},
    ]
    draw_stream_table(d, events)
    draw_status_bar(d, "⚡ Ingress event received via Redis XADD :6379")
    return img


def render_frame_3():
    img = Image.new('RGB', (WIDTH, HEIGHT), BG_COLOR)
    d = ImageDraw.Draw(img)
    draw_window_frame(d)
    draw_header_and_tabs(d, active_tab=1)
    events = [
        {"time": "11:25:01", "topic": "orders", "part": 0, "offset": 0, "key": "ord-101", "preview": '{"sku":"LAPTOP-X","qty":1,"price":1299.99}'},
        {"time": "11:25:02", "topic": "telemetry", "part": 0, "offset": 0, "key": "dev-alpha", "preview": '{"sensor":"DHT22","temp":24.2,"status":"ok"}'},
        {"time": "11:25:03", "topic": "notifications", "part": 0, "offset": 0, "key": "usr-88", "preview": '{"channel":"email","to":"dev@startup.io"}'},
    ]
    draw_stream_table(d, events)
    draw_status_bar(d)
    return img


def render_frame_4():
    img = Image.new('RGB', (WIDTH, HEIGHT), BG_COLOR)
    d = ImageDraw.Draw(img)
    draw_window_frame(d)
    draw_header_and_tabs(d, active_tab=1)
    events = [
        {"time": "11:25:01", "topic": "orders", "part": 0, "offset": 0, "key": "ord-101", "preview": '{"sku":"LAPTOP-X","qty":1,"price":1299.99}'},
        {"time": "11:25:02", "topic": "telemetry", "part": 0, "offset": 0, "key": "dev-alpha", "preview": '{"sensor":"DHT22","temp":24.2,"status":"ok"}'},
        {"time": "11:25:03", "topic": "notifications", "part": 0, "offset": 0, "key": "usr-88", "preview": '{"channel":"email","to":"dev@startup.io"}'},
        {"time": "11:25:05", "topic": "payments.dlq", "part": 0, "offset": 0, "key": "txn-904", "preview": '{"error":"INVALID_CURRENCY","amount":450000}'},
    ]
    draw_stream_table(d, events)
    draw_status_bar(d, "🚨 Message routed to DLQ (Consumer failed)")
    return img


def render_frame_5():
    # Selection on DLQ row
    img = Image.new('RGB', (WIDTH, HEIGHT), BG_COLOR)
    d = ImageDraw.Draw(img)
    draw_window_frame(d)
    draw_header_and_tabs(d, active_tab=1)
    events = [
        {"time": "11:25:01", "topic": "orders", "part": 0, "offset": 0, "key": "ord-101", "preview": '{"sku":"LAPTOP-X","qty":1,"price":1299.99}'},
        {"time": "11:25:02", "topic": "telemetry", "part": 0, "offset": 0, "key": "dev-alpha", "preview": '{"sensor":"DHT22","temp":24.2,"status":"ok"}'},
        {"time": "11:25:03", "topic": "notifications", "part": 0, "offset": 0, "key": "usr-88", "preview": '{"channel":"email","to":"dev@startup.io"}'},
        {"time": "11:25:05", "topic": "payments.dlq", "part": 0, "offset": 0, "key": "txn-904", "preview": '{"error":"INVALID_CURRENCY","amount":450000}'},
    ]
    draw_stream_table(d, events, selected_index=3)
    draw_status_bar(d, "Selected: 🚨 payments.dlq [offset 0]", "Enter: Inspect • r/e: Replay & Edit • Tab: Views")
    return img


def render_frame_6():
    # Inspect modal
    img = Image.new('RGB', (WIDTH, HEIGHT), BG_COLOR)
    d = ImageDraw.Draw(img)
    draw_window_frame(d)
    draw_header_and_tabs(d, active_tab=1)
    events = [
        {"time": "11:25:01", "topic": "orders", "part": 0, "offset": 0, "key": "ord-101", "preview": '{"sku":"LAPTOP-X","qty":1,"price":1299.99}'},
        {"time": "11:25:02", "topic": "telemetry", "part": 0, "offset": 0, "key": "dev-alpha", "preview": '{"sensor":"DHT22","temp":24.2,"status":"ok"}'},
        {"time": "11:25:03", "topic": "notifications", "part": 0, "offset": 0, "key": "usr-88", "preview": '{"channel":"email","to":"dev@startup.io"}'},
        {"time": "11:25:05", "topic": "payments.dlq", "part": 0, "offset": 0, "key": "txn-904", "preview": '{"error":"INVALID_CURRENCY","amount":450000}'},
    ]
    draw_stream_table(d, events, selected_index=3)
    
    inspect_lines = [
        ("TOPIC", "payments.dlq", RED),
        ("PARTITION", "0", TEXT_WHITE),
        ("OFFSET", "0", GREEN),
        ("TIME", "2026-11-14T11:25:05Z", TEXT_MUTED),
        ("KEY", "txn-904", CYAN),
        ("HEADERS", "x-retry-count=3, x-source-service=checkout-api", TEXT_MUTED),
        "",
        "PAYLOAD (Formatted JSON):",
        "{",
        '  "transaction_id": "txn-904",',
        '  "amount": 450000.00,',
        '  "currency": "IDR_OLD",      <-- Malformed currency code',
        '  "status": "REJECTED_BY_GATEWAY"',
        "}",
    ]
    draw_modal(d, "🔍 PAYLOAD INSPECTOR MODAL", inspect_lines, is_error=True)
    draw_status_bar(d, "", "Esc/Enter: Close • r/e: Edit & Replay in Studio")
    return img


def render_frame_7():
    # Replay Studio Open (Initial with error payload)
    img = Image.new('RGB', (WIDTH, HEIGHT), BG_COLOR)
    d = ImageDraw.Draw(img)
    draw_window_frame(d)
    draw_header_and_tabs(d, active_tab=1)
    events = [
        {"time": "11:25:01", "topic": "orders", "part": 0, "offset": 0, "key": "ord-101", "preview": '{"sku":"LAPTOP-X","qty":1,"price":1299.99}'},
        {"time": "11:25:02", "topic": "telemetry", "part": 0, "offset": 0, "key": "dev-alpha", "preview": '{"sensor":"DHT22","temp":24.2,"status":"ok"}'},
        {"time": "11:25:03", "topic": "notifications", "part": 0, "offset": 0, "key": "usr-88", "preview": '{"channel":"email","to":"dev@startup.io"}'},
        {"time": "11:25:05", "topic": "payments.dlq", "part": 0, "offset": 0, "key": "txn-904", "preview": '{"error":"INVALID_CURRENCY","amount":450000}'},
    ]
    draw_stream_table(d, events, selected_index=3)
    
    replay_lines = [
        ("TARGET TOPIC", "payments (auto-stripped .dlq suffix)", GREEN),
        ("KEY", "txn-904", CYAN),
        "",
        "EDIT PAYLOAD IN-PLACE (Press Ctrl+S to republish):",
        "{",
        '  "transaction_id": "txn-904",',
        '  "amount": 450000.00,',
        '  "currency": "IDR_OLD",      <-- Editing here...',
        '  "status": "REJECTED_BY_GATEWAY"',
        "}",
    ]
    draw_modal(d, "🛠️ DLQ REPLAY STUDIO (In-Terminal Fix)", replay_lines, is_error=True)
    draw_status_bar(d, "Editing DLQ record in terminal", "Ctrl+S: Submit Replay • Esc: Cancel")
    return img


def render_frame_8():
    # Replay Studio Payload Fixed
    img = Image.new('RGB', (WIDTH, HEIGHT), BG_COLOR)
    d = ImageDraw.Draw(img)
    draw_window_frame(d)
    draw_header_and_tabs(d, active_tab=1)
    events = [
        {"time": "11:25:01", "topic": "orders", "part": 0, "offset": 0, "key": "ord-101", "preview": '{"sku":"LAPTOP-X","qty":1,"price":1299.99}'},
        {"time": "11:25:02", "topic": "telemetry", "part": 0, "offset": 0, "key": "dev-alpha", "preview": '{"sensor":"DHT22","temp":24.2,"status":"ok"}'},
        {"time": "11:25:03", "topic": "notifications", "part": 0, "offset": 0, "key": "usr-88", "preview": '{"channel":"email","to":"dev@startup.io"}'},
        {"time": "11:25:05", "topic": "payments.dlq", "part": 0, "offset": 0, "key": "txn-904", "preview": '{"error":"INVALID_CURRENCY","amount":450000}'},
    ]
    draw_stream_table(d, events, selected_index=3)
    
    replay_lines = [
        ("TARGET TOPIC", "payments", GREEN),
        ("KEY", "txn-904", CYAN),
        "",
        "EDIT PAYLOAD IN-PLACE (Press Ctrl+S to republish):",
        "{",
        '  "transaction_id": "txn-904",',
        '  "amount": 450000.00,',
        '  "currency": "IDR",          ✅ Fixed!',
        '  "status": "APPROVED"        ✅ Fixed!',
        "}",
    ]
    draw_modal(d, "🛠️ DLQ REPLAY STUDIO (In-Terminal Fix)", replay_lines)
    draw_status_bar(d, "Ready to republish fixed message", "Press Ctrl+S to Republish • Esc: Cancel")
    return img


def render_frame_9():
    # Replay Success & Live Stream Updated with new event
    img = Image.new('RGB', (WIDTH, HEIGHT), BG_COLOR)
    d = ImageDraw.Draw(img)
    draw_window_frame(d)
    draw_header_and_tabs(d, active_tab=1)
    events = [
        {"time": "11:25:01", "topic": "orders", "part": 0, "offset": 0, "key": "ord-101", "preview": '{"sku":"LAPTOP-X","qty":1,"price":1299.99}'},
        {"time": "11:25:02", "topic": "telemetry", "part": 0, "offset": 0, "key": "dev-alpha", "preview": '{"sensor":"DHT22","temp":24.2,"status":"ok"}'},
        {"time": "11:25:03", "topic": "notifications", "part": 0, "offset": 0, "key": "usr-88", "preview": '{"channel":"email","to":"dev@startup.io"}'},
        {"time": "11:25:05", "topic": "payments.dlq", "part": 0, "offset": 0, "key": "txn-904", "preview": '{"error":"INVALID_CURRENCY","amount":450000}'},
        {"time": "11:25:12", "topic": "payments", "part": 0, "offset": 1, "key": "txn-904", "preview": '{"transaction_id":"txn-904","currency":"IDR","status":"APPROVED"}'},
    ]
    draw_stream_table(d, events, selected_index=4)
    draw_status_bar(d, "✅ Replayed to topic 'payments' at 11:25:12 (Republished!)", "Tab: Switch View • Enter: Inspect • q: Quit")
    return img


def render_frame_10():
    # Tab 2: Topics & DLQ View
    img = Image.new('RGB', (WIDTH, HEIGHT), BG_COLOR)
    d = ImageDraw.Draw(img)
    draw_window_frame(d)
    draw_header_and_tabs(d, active_tab=2)
    topics = [
        {"name": "orders", "parts": 1, "total": 1, "earliest": 0, "latest": 0},
        {"name": "telemetry", "parts": 1, "total": 1, "earliest": 0, "latest": 0},
        {"name": "notifications", "parts": 1, "total": 1, "earliest": 0, "latest": 0},
        {"name": "payments", "parts": 1, "total": 2, "earliest": 0, "latest": 1},
        {"name": "payments.dlq", "parts": 1, "total": 1, "earliest": 0, "latest": 0},
    ]
    draw_topics_table(d, topics)
    draw_status_bar(d, "Total: 5 Topics (1 Dead-Letter Queue detected)", "Tab: Switch View • q: Quit")
    return img


def main():
    print("Generating demo frames...")
    frames_config = [
        (render_frame_1(), 900),   # 1st event
        (render_frame_2(), 800),   # 2nd event
        (render_frame_3(), 800),   # 3rd event
        (render_frame_4(), 1100),  # DLQ event arrives
        (render_frame_5(), 900),   # Navigate & select DLQ row
        (render_frame_6(), 1600),  # Inspect modal
        (render_frame_7(), 1200),  # Replay studio opened
        (render_frame_8(), 1400),  # Payload edited in-place
        (render_frame_9(), 1800),  # Republished & live stream updated!
        (render_frame_10(), 2200), # Topics & DLQ summary tab
    ]

    # Save Hero Image (Static PNG) for fast loading
    hero_img = frames_config[5][0] # Inspect / Replay Studio state
    hero_path = "docs/assets/pocketqueue-tui.png"
    hero_img.save(hero_path, optimize=True)
    print(f"Saved hero image: {hero_path}")

    # Build animated GIF using imageio
    frames_np = [np.array(f[0]) for f in frames_config]
    durations_ms = [f[1] for f in frames_config]

    gif_path = "docs/assets/pocketqueue-demo.gif"
    iio.imwrite(gif_path, frames_np, duration=durations_ms, loop=0)
    print(f"Saved animated GIF: {gif_path}")
    print(f"GIF size: {os.path.getsize(gif_path) / 1024:.1f} KB")


if __name__ == "__main__":
    main()
