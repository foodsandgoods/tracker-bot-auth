"""
Unified formatters for issue lists and Markdown handling.
"""

from dataclasses import dataclass
import re


@dataclass
class ListFormatConfig:
    """Configuration for issue list formatting."""
    summary_limit: int = 50      # Max chars for summary
    show_status: bool = True     # Show status under each issue
    show_number: bool = True     # Show issue number (1. 2. 3.)
    items_limit: int | None = None  # Max items to show (None = all)


# Preset configurations
FORMAT_MORNING = ListFormatConfig(summary_limit=50, show_status=True)
FORMAT_EVENING = ListFormatConfig(summary_limit=50, show_status=False)
FORMAT_DEFAULT = ListFormatConfig(summary_limit=50, show_status=True)

# For reminder notifications (short lists, 3 items max)
FORMAT_REMINDER = ListFormatConfig(summary_limit=40, show_status=False, items_limit=3)

# For background workers (morning/evening auto-send, 10 items max)
FORMAT_WORKER = ListFormatConfig(summary_limit=40, show_status=False, items_limit=10)


def escape_md(text: str) -> str:
    """Escape Markdown special characters for Telegram Markdown (not MarkdownV2)."""
    if not text:
        return ""
    # For regular Markdown: replace [ ] with ( ) to avoid breaking links
    # Don't escape other chars as they're safe in regular Markdown
    return text.replace("[", "(").replace("]", ")")


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
        issues: List of issue dicts with keys: key, summary, status, url
        title: Header line (e.g. "🌅 *Утренний отчёт — INV* (24.01.2026, 8 задач)")
        config: Formatting configuration
    
    Returns:
        Formatted Markdown string
    """
    lines = [title]
    
    items = issues[:config.items_limit] if config.items_limit else issues
    
    for idx, issue in enumerate(items, 1):
        key = issue.get("key", "")
        summary = escape_md(issue.get("summary", "")[:config.summary_limit])
        url = issue.get("url", f"https://tracker.yandex.ru/{key}")
        
        if config.show_number:
            lines.append(f"{idx}. [{key}]({url}): {summary}")
        else:
            lines.append(f"[{key}]({url}): {summary}")
        
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
