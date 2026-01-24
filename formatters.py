"""
Unified formatters for issue lists and Markdown handling.
"""

from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Optional


@dataclass
class ListFormatConfig:
    """Configuration for issue list formatting."""
    summary_limit: int = 50      # Max chars for summary
    show_status: bool = True     # Show status under each issue
    show_number: bool = True     # Show issue number (1. 2. 3.)
    show_date: bool = True       # Show date/time of last update
    items_limit: int | None = None  # Max items to show (None = all)


# Preset configurations
FORMAT_MORNING = ListFormatConfig(summary_limit=50, show_status=True, show_date=True)
FORMAT_EVENING = ListFormatConfig(summary_limit=50, show_status=True, show_date=True)
FORMAT_DEFAULT = ListFormatConfig(summary_limit=50, show_status=True, show_date=True)

# For reminder notifications (short lists, 3 items max)
FORMAT_REMINDER = ListFormatConfig(summary_limit=40, show_status=False, show_date=True, items_limit=3)

# For background workers (morning/evening auto-send, 10 items max)
FORMAT_WORKER = ListFormatConfig(summary_limit=40, show_status=True, show_date=True, items_limit=10)


def fmt_date(date_str: Optional[str]) -> str:
    """Format ISO date to DD.MM.YYYY HH:MM in Moscow timezone (UTC+3)."""
    if not date_str:
        return ""
    try:
        clean = date_str.replace("Z", "+00:00")
        if "+" in clean and ":" not in clean.split("+")[-1]:
            parts = clean.rsplit("+", 1)
            if len(parts[1]) == 4:
                clean = f"{parts[0]}+{parts[1][:2]}:{parts[1][2:]}"
        dt = datetime.fromisoformat(clean)
        # Convert to Moscow time (UTC+3)
        moscow_tz = timezone(timedelta(hours=3))
        dt_moscow = dt.astimezone(moscow_tz)
        return dt_moscow.strftime("%d.%m.%Y %H:%M")
    except Exception:
        return date_str[:16] if len(date_str) > 16 else date_str


def escape_md(text: str) -> str:
    """Escape Markdown special characters for Telegram Markdown (not MarkdownV2)."""
    if not text:
        return ""
    # For regular Markdown: replace [ ] with ( ) to avoid breaking links
    # Don't escape other chars as they're safe in regular Markdown
    return text.replace("[", "(").replace("]", ")")


def fmt_issue_link(
    issue: dict,
    prefix: str = "",
    show_date: bool = True,
    summary_limit: int = 55
) -> str:
    """
    Format issue as Markdown hyperlink.
    
    Args:
        issue: Dict with keys: key, summary, url, updatedAt
        prefix: Optional prefix (e.g. "✅ " or "⏳ ")
        show_date: Whether to show date
        summary_limit: Max chars for summary
    
    Returns:
        Formatted string: [KEY: Summary](url) (date)
    """
    key = issue.get("key", "")
    summary = escape_md((issue.get("summary") or "")[:summary_limit])
    url = issue.get("url") or f"https://tracker.yandex.ru/{key}"
    date_str = fmt_date(issue.get("updatedAt")) if show_date else ""
    
    link = f"{prefix}[{key}: {summary}]({url})"
    return f"{link} ({date_str})" if date_str else link


def strip_markdown(text: str) -> str:
    """Remove all Markdown formatting from text."""
    return (text
            .replace("*", "")
            .replace("_", "")
            .replace("[", "")
            .replace("]", "")
            .replace("(", " ")
            .replace(")", ""))


def format_issue_list(
    issues: list[dict],
    title: str,
    config: ListFormatConfig = FORMAT_DEFAULT
) -> str:
    """
    Format a list of issues into Markdown text.
    
    Args:
        issues: List of issue dicts with keys: key, summary, status, url, updatedAt
        title: Header line (e.g. "🌅 *Утренний отчёт — INV* (24.01.2026, 8 задач)")
        config: Formatting configuration
    
    Returns:
        Formatted Markdown string
    """
    lines = [title]
    
    items = issues[:config.items_limit] if config.items_limit else issues
    
    for idx, issue in enumerate(items, 1):
        # Use unified fmt_issue_link
        link = fmt_issue_link(
            issue,
            prefix="",
            show_date=config.show_date,
            summary_limit=config.summary_limit
        )
        
        if config.show_number:
            lines.append(f"{idx}. {link}")
        else:
            lines.append(link)
        
        if config.show_status:
            status = escape_md(issue.get("status", ""))
            if status:
                lines.append(f"   _{status}_")
    
    return "\n".join(lines)


async def safe_edit_markdown(
    message,
    text: str,
    reply_markup=None,
    max_length: int = 4000
) -> None:
    """
    Edit message with Markdown, fallback to plain text on error.
    
    Args:
        message: Telegram message to edit
        text: Markdown text
        reply_markup: Optional keyboard markup
        max_length: Max message length
    """
    try:
        await message.edit_text(
            text[:max_length],
            parse_mode="Markdown",
            reply_markup=reply_markup
        )
    except Exception:
        # Fallback without Markdown if parsing fails
        await message.edit_text(
            strip_markdown(text)[:max_length],
            reply_markup=reply_markup
        )


async def safe_send_markdown(
    bot,
    chat_id: int,
    text: str,
    max_length: int = 4000
) -> None:
    """
    Send message with Markdown, fallback to plain text on error.
    
    Args:
        bot: Telegram Bot instance
        chat_id: Chat ID to send to
        text: Markdown text
        max_length: Max message length
    """
    try:
        await bot.send_message(
            chat_id,
            text[:max_length],
            parse_mode="Markdown"
        )
    except Exception:
        # Fallback without Markdown if parsing fails
        await bot.send_message(
            chat_id,
            strip_markdown(text)[:max_length]
        )
