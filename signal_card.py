"""Reference-style PNG cards from copied signal observations, never broker I/O."""
import io
from functools import lru_cache

from PIL import Image, ImageDraw, ImageFont, ImageFilter, ImageOps
from entry_confirmation import number, ENTRY_WINDOW_SECONDS

SIZE = (1000, 800)


@lru_cache(maxsize=12)
def font(size):
    # Pillow's bundled scalable Aileron face works without system font paths.
    return ImageFont.load_default(size=size)


@lru_cache(maxsize=1)
def background():
    image = Image.new("RGB", SIZE, "#080b12")
    glow = Image.new("RGB", SIZE, "#000000")
    draw = ImageDraw.Draw(glow)
    draw.ellipse((-180, -100, 470, 420), fill="#141333")
    draw.ellipse((720, 470, 1230, 1050), fill="#082320")
    image = Image.blend(image, glow.filter(ImageFilter.GaussianBlur(120)), 0.32)
    draw = ImageDraw.Draw(image)
    draw.rounded_rectangle((60, 80, 940, 740), radius=42, fill="#17171f")

    vertical = Image.linear_gradient("L").resize(SIZE)
    horizontal = Image.linear_gradient("L").rotate(270).resize(SIZE)
    left = ImageOps.colorize(vertical, "#6679f5", "#dd74f5")
    right = ImageOps.colorize(vertical, "#ac72e8", "#40bdae")
    gradient = Image.composite(left, right, horizontal)
    outline = Image.new("L", SIZE, 0)
    mask = ImageDraw.Draw(outline)
    mask.rounded_rectangle((60, 80, 940, 740), radius=42, fill=255)
    mask.rounded_rectangle((62, 82, 938, 738), radius=40, fill=0)
    image.paste(gradient, (0, 0), outline)
    return image


def quote_text(value):
    return f"{value:.8f}".rstrip("0").rstrip(".") if number(value) and value > 0 else "Unavailable"


def render_signal_card(event, preview=False):
    """No fills, confidence scores or results are invented for the card."""
    image = background().copy()
    draw = ImageDraw.Draw(image)
    muted, white, accent = "#999bad", "#f1f1f7", "#b9b5ef"

    def text(x, y, value, size=29, color=white, bold=False, width=365):
        value = str(value)
        while size > 16 and draw.textlength(value, font=font(size)) > width:
            size -= 1
        draw.text((x, y), value, font=font(size), fill=color,
                  stroke_width=1 if bold else 0, stroke_fill=color)

    def label(x, y, value):
        text(x, y, value, size=22, color=muted)

    if preview:
        text(60, 28, "PREVIEW · ILLUSTRATIVE ONLY", size=19, color=accent, width=880)
    text(112, 132, "NEW SIGNAL", size=44, bold=True, width=660)
    badge = "LIVE" if event["mode"] == "REAL" else "DEMO"
    draw.rounded_rectangle((784, 131, 890, 172), radius=12, fill="#262633")
    text(804, 141, badge, size=19, color=accent, width=80)

    label(112, 207, "DIRECTION")
    label(524, 207, "PAIR")
    direction = "BUY / CALL" if event["side"] == "CALL" else "SELL / PUT"
    color = "#59d4b0" if event["side"] == "CALL" else "#ee7597"
    text(112, 240, direction, color=color)
    text(524, 240, event["pair"])

    label(112, 300, "CONFIRMED QUOTE")
    label(524, 300, "STRATEGY")
    text(112, 333, quote_text(event.get("quote")))
    strategy = "FCB BREAKOUT" if event["kind"] == "FCB" else "FCB CONTINUATION"
    text(524, 333, strategy, color=accent)
    label(112, 392, "CONFIGURED STAKE")
    stake = event.get("stake")
    text(112, 425, f"${stake:.2f}" if number(stake) and stake > 0 else "Unavailable", color="#c5c5d4")

    draw.line((112, 482, 890, 482), fill="#56556c", width=1)
    text(112, 535, "SIGNAL DETAILS", size=24, color=accent, bold=True)
    label(112, 597, "SIGNAL TYPE")
    label(524, 597, "INSTRUMENT")
    text(112, 630, "OTC Binary" if event["pair"].endswith("-OTC") else "Binary")
    text(524, 630, "TURBO · 1 MIN", color=accent)
    text(112, 697, f"Signal only · Quote is not a fill · Entry window 0-{ENTRY_WINDOW_SECONDS:g}s",
         size=18, color=muted, width=780)
    output = io.BytesIO()
    image.save(output, format="PNG", optimize=True)
    return output.getvalue()
