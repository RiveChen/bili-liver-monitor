"""Commands: "反向" (flip) and "对称" (left-half → mirror to right)."""

from __future__ import annotations

import asyncio
import base64
import html as html_mod
import logging
import re
from io import BytesIO
from typing import TYPE_CHECKING, Callable

import aiohttp
from PIL import Image

from .base import Command, MessageContext

if TYPE_CHECKING:
    pass

log = logging.getLogger(__name__)

# CQ code regex (shared)
RE_CQ_IMAGE = re.compile(r"\[CQ:image[^\]]*?url=(?P<url>[^\],]+)")
RE_CQ_IMAGE_FILE = re.compile(r"\[CQ:image[^\]]*?file=(?P<file>[^,\]]+)")
RE_CQ_REPLY = re.compile(r"\[CQ:reply[^\]]*?id=(?P<id>\d+)")
RE_CQ_AT = re.compile(r"\[CQ:at[^\]]*?qq=(?P<qq>\d+)")


# ═══════════════════════════════════════════════════════════════════
#  Shared helpers (module-level, not tied to a class)
# ═══════════════════════════════════════════════════════════════════


def _is_at_bot(raw_message: str, bot_qq: str) -> bool:
    for qq in RE_CQ_AT.findall(raw_message):
        if qq == bot_qq or qq == "all":
            return True
    return False


def _extract_images(message: str) -> list[str]:
    urls = RE_CQ_IMAGE.findall(message)
    return [html_mod.unescape(u) for u in urls]


def _extract_image_files(message: str) -> list[str]:
    return RE_CQ_IMAGE_FILE.findall(message)


def _extract_reply_id(message: str) -> int | None:
    m = RE_CQ_REPLY.search(message)
    return int(m.group("id")) if m else None


async def _download_image(
    ctx: MessageContext, url: str, file_id: str = ""
) -> bytes | None:
    url = html_mod.unescape(url)

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Referer": "https://multimedia.nt.qq.com.cn/",
    }

    session: aiohttp.ClientSession = ctx.session  # type: ignore[assignment]

    # Try direct
    try:
        async with session.get(
            url,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=15.0),
            allow_redirects=True,
        ) as resp:
            if resp.status == 200:
                data = await resp.read()
                if data:
                    return data
            log.warning("[CMD] Direct download HTTP %d: %s", resp.status, url[:60])
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        log.warning("[CMD] Direct download failed: %s — %s", e, url[:60])

    # Fallback via get_image API
    if file_id and ctx.pusher:
        log.info("[CMD] Falling back to get_image API: %s…", file_id[:40])
        return await _download_image_via_api(ctx, file_id)

    return None


async def _download_image_via_api(ctx: MessageContext, file_id: str) -> bytes | None:
    if not ctx.pusher:
        return None

    api_url = ctx.pusher.api_url
    token = ctx.pusher.token
    session: aiohttp.ClientSession = ctx.session  # type: ignore[assignment]

    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    try:
        async with session.post(
            f"{api_url}/get_image",
            headers=headers,
            json={"file_id": file_id},
            timeout=aiohttp.ClientTimeout(total=15.0),
        ) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
            if data.get("retcode") != 0:
                return None
            img_info = data.get("data", {})
            download_url = img_info.get("url", "") or img_info.get("file", "")
            if not download_url:
                return None

        dl_headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Referer": "https://multimedia.nt.qq.com.cn/",
        }
        async with session.get(
            download_url,
            headers=dl_headers,
            timeout=aiohttp.ClientTimeout(total=15.0),
            allow_redirects=True,
        ) as resp:
            if resp.status == 200:
                data = await resp.read()
                if data:
                    return data
            return None
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        log.warning("[CMD] get_image API failed: %s", e)
        return None


async def _fetch_reply_images(
    ctx: MessageContext, reply_id: int
) -> list[tuple[str, str]]:
    if not ctx.pusher:
        return []

    api_url = ctx.pusher.api_url
    token = ctx.pusher.token
    session: aiohttp.ClientSession = ctx.session  # type: ignore[assignment]

    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    try:
        async with session.post(
            f"{api_url}/get_msg",
            headers=headers,
            json={"message_id": reply_id},
            timeout=aiohttp.ClientTimeout(total=15.0),
        ) as resp:
            if resp.status != 200:
                return []
            data = await resp.json()
            if data.get("retcode") != 0:
                return []
            msg_data = data.get("data", {})
            message = msg_data.get("message", "")

            images: list[tuple[str, str]] = []
            if isinstance(message, list):
                for seg in message:
                    if seg.get("type") == "image":
                        seg_data = seg.get("data", {})
                        url = html_mod.unescape(seg_data.get("url", ""))
                        file_id = seg_data.get("file", "")
                        images.append((url, file_id))
            else:
                msg_str = str(message)
                urls = _extract_images(msg_str)
                files = _extract_image_files(msg_str)
                for i, url in enumerate(urls):
                    fid = files[i] if i < len(files) else ""
                    images.append((url, fid))
            return images
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        log.warning("[CMD] get_msg request failed: %s", e)
        return []


async def _send_raw(ctx: MessageContext, endpoint: str, payload: dict) -> bool:
    if not ctx.pusher:
        return False
    api_url = ctx.pusher.api_url
    token = ctx.pusher.token
    session: aiohttp.ClientSession = ctx.session  # type: ignore[assignment]

    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    try:
        async with session.post(
            f"{api_url}/{endpoint}",
            headers=headers,
            json=payload,
            timeout=aiohttp.ClientTimeout(total=15.0),
        ) as resp:
            if resp.status in (200, 204):
                return True
            body = await resp.text()
            log.warning(
                "[CMD] _send_raw %s HTTP %d — %s", endpoint, resp.status, body[:200]
            )
            return False
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        log.warning("[CMD] _send_raw %s failed: %s", endpoint, e)
        return False


# ═══════════════════════════════════════════════════════════════════
#  Image processing functions
# ═══════════════════════════════════════════════════════════════════


def _process_images_frame_by_frame(
    img: Image.Image,
    processor: Callable[[Image.Image], Image.Image],
) -> bytes | Image.Image:
    """Apply *processor* to each frame of an image, preserving GIF animation."""
    if getattr(img, "is_animated", False) and getattr(img, "n_frames", 1) > 1:
        frames: list[Image.Image] = []
        durations: list[int] = []
        max_frames = min(getattr(img, "n_frames", 1), 100)

        for frame in range(max_frames):
            img.seek(frame)

            # 1. Convert to RGBA and let processor handle it
            frame_rgba = img.convert("RGBA")
            processed_rgba = processor(frame_rgba)

            # 2. Extract alpha and binarize (0=transparent, 255=opaque)
            r, g, b, a = processed_rgba.split()
            binary_alpha = a.point(lambda p: 255 if p > 127 else 0)  # type: ignore[operator]

            # 3. Composite on black bg → pure RGB (eliminate semi-transparent edges)
            black_bg = Image.new("RGB", processed_rgba.size, (0, 0, 0))
            clean_rgb = Image.composite(
                processed_rgba.convert("RGB"), black_bg, binary_alpha
            )

            # 4. High-quality quantization (method=0), limit to 255 colors
            quantized_rgb = clean_rgb.quantize(
                colors=255, method=Image.Quantize.MEDIANCUT, dither=Image.Dither.NONE
            )

            # 5. Extract palette, pad to 768 bytes (256 colors * 3 channels)
            palette = quantized_rgb.getpalette()
            if palette:
                palette = (palette + [0] * 768)[:768]

            # 6. Create new P-mode image with index 255 as background
            final_frame = Image.new("P", processed_rgba.size, color=255)
            if palette:
                final_frame.putpalette(palette)

            # 7. Paste quantized image using binary_alpha mask
            final_frame.paste(quantized_rgb, mask=binary_alpha)

            frames.append(final_frame)
            durations.append(img.info.get("duration", 100))

        buf = BytesIO()

        # 8. Save as GIF with index 255 as transparent
        frames[0].save(
            buf,
            format="GIF",
            save_all=True,
            append_images=frames[1:],
            duration=durations,
            loop=img.info.get("loop", 0),
            disposal=2,
            transparency=255,
        )
        return buf.getvalue()
    else:
        return processor(img.copy())


def _mirror_image(img: Image.Image) -> Image.Image:
    """Flip entire image left-to-right."""
    return img.transpose(Image.Transpose.FLIP_LEFT_RIGHT)


def _flip_upside_down(img: Image.Image) -> Image.Image:
    """Flip entire image upside down (top-to-bottom)."""
    return img.transpose(Image.Transpose.FLIP_TOP_BOTTOM)


def _make_symmetric(img: Image.Image) -> Image.Image:
    """Crop left half, mirror it, paste both halves."""
    w, h = img.size
    half = w // 2
    left = img.crop((0, 0, half, h))
    mirrored = left.transpose(Image.Transpose.FLIP_LEFT_RIGHT)

    result = Image.new("RGBA", (w, h))
    result.paste(left.convert("RGBA"), (0, 0))
    result.paste(mirrored.convert("RGBA"), (half, 0))

    return result


async def _process_image(
    image_data: bytes, processor: Callable[[Image.Image], Image.Image]
) -> bytes | None:
    """Download, process (via *processor*), re-encode."""
    try:
        img = Image.open(BytesIO(image_data))
        result = _process_images_frame_by_frame(img, processor)

        if isinstance(result, bytes):
            # Already encoded (GIF animation)
            return result
        # Static image → PNG
        assert isinstance(result, Image.Image)
        buf = BytesIO()
        result.save(buf, format="PNG")
        return buf.getvalue()
    except Exception as e:
        log.warning("[CMD] Image processing failed: %s", e)
        return None


# ═══════════════════════════════════════════════════════════════════
#  Base class for both commands (common execute flow)
# ═══════════════════════════════════════════════════════════════════


class BaseSymCommand(Command):
    """Base class for "反向" and "对称" commands.

    Subclasses must set *keyword* and *processor*.
    """

    keyword: str = ""
    processor: Callable[[Image.Image], Image.Image] = staticmethod(lambda img: img)  # type: ignore[assignment]

    def match(self, ctx: MessageContext) -> bool:
        """Return True if this message should trigger."""
        if self.keyword not in ctx.plain_text:
            return False

        if ctx.is_private():
            return True
        elif ctx.is_group():
            if _is_at_bot(ctx.raw_message, ctx.bot_qq):
                return True
            if _extract_reply_id(ctx.raw_message) is not None:
                return True
            return False
        return False

    async def execute(self, ctx: MessageContext) -> None:
        """Download images, process, send back."""
        # ── Group whitelist ──
        if ctx.is_group() and ctx.allowed_groups is not None:
            if ctx.group_id not in ctx.allowed_groups:
                return

        log.info(
            "[%s] Processing user=%s type=%s msg_id=%d",
            self.keyword,
            ctx.user_id,
            ctx.message_type,
            ctx.message_id,
        )

        # ── Extract images ──
        image_urls = _extract_images(ctx.raw_message)
        image_files = _extract_image_files(ctx.raw_message)
        reply_id = _extract_reply_id(ctx.raw_message)

        if not image_urls and reply_id is not None:
            log.info("[%s] Fetching reply %d for images", self.keyword, reply_id)
            reply_images = await _fetch_reply_images(ctx, reply_id)
            for url, fid in reply_images:
                image_urls.append(url)
                image_files.append(fid)

        if not image_urls:
            log.info("[%s] No images found", self.keyword)
            return

        # ── Process images ──
        processed_images: list[bytes] = []
        for i, url in enumerate(image_urls[:5]):
            file_id = image_files[i] if i < len(image_files) else ""
            log.info(
                "[%s] Downloading %d/%d: url=%s… file=%s…",
                self.keyword,
                i + 1,
                min(len(image_urls), 5),
                url[:60] if url else "(none)",
                file_id[:40] if file_id else "(none)",
            )
            img_data = await _download_image(ctx, url, file_id)
            if img_data is None:
                continue
            result = await _process_image(img_data, self.processor)
            if result is not None:
                processed_images.append(result)

        if not processed_images:
            log.warning("[%s] No images successfully processed", self.keyword)
            return

        # ── Build reply ──
        reply_prefix = f"[CQ:reply,id={ctx.message_id}]" if ctx.is_group() else ""

        image_segments = []
        for img_bytes in processed_images:
            b64 = base64.b64encode(img_bytes).decode("ascii")
            image_segments.append(f"[CQ:image,file=base64://{b64}]")

        response_msg = reply_prefix + "".join(image_segments)

        # ── Send ──
        if ctx.is_private():
            await _send_raw(
                ctx,
                "send_private_msg",
                {
                    "user_id": ctx.user_id,
                    "message": response_msg,
                },
            )
        elif ctx.is_group() and ctx.group_id:
            await _send_raw(
                ctx,
                "send_group_msg",
                {
                    "group_id": ctx.group_id,
                    "message": response_msg,
                },
            )

        log.info(
            "[%s] Sent %d image(s) to user=%s",
            self.keyword,
            len(processed_images),
            ctx.user_id,
        )


# ═══════════════════════════════════════════════════════════════════
#  Concrete commands
# ═══════════════════════════════════════════════════════════════════


class FlipCommand(BaseSymCommand):
    """Flip entire image left-to-right ("反向")."""

    name = "反向"
    keyword = "反向"
    processor = staticmethod(_mirror_image)


class SymmetryCommand(BaseSymCommand):
    """Crop left half and mirror to right ("对称")."""

    name = "对称"
    keyword = "对称"
    processor = staticmethod(_make_symmetric)


class FlipUpsideDownCommand(BaseSymCommand):
    """Flip entire image upside down ("倒立")."""

    name = "倒立"
    keyword = "倒立"
    processor = staticmethod(_flip_upside_down)
