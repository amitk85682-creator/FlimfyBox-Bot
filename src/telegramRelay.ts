import { logEvent } from './logger.ts';
import type { AppUser } from './store.ts';

export interface TelegramRelayResult {
  relayed: boolean;
  relay_status: 'sent' | 'pending';
  telegram_chat_id: string | null;
  telegram_message_id: number | null;
}

export function escapeTelegramHtml(input: string): string {
  return String(input || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

export function formatGlobalChatTelegramText(user: AppUser, message: string): string {
  const displayName = escapeTelegramHtml(user.first_name || 'Community Member');
  const handle = user.username ? ` (@${escapeTelegramHtml(user.username)})` : '';
  const body = escapeTelegramHtml(message.trim().slice(0, 3500));
  return `💬 <b>${displayName}${handle}</b> <i>[Mini App]</i>\n${body}`;
}

export async function relayGlobalChatToTelegram(
  user: AppUser,
  message: string,
  requestId?: string,
): Promise<TelegramRelayResult> {
  const botToken = (process.env.TELEGRAM_BOT_TOKEN || '').trim();
  const groupChatId = (process.env.GROUP_CHAT_ID || process.env.REQUIRED_GROUP_ID || '').trim();

  if (!botToken || !groupChatId || !/^-?\d+$/.test(groupChatId)) {
    return {
      relayed: false,
      relay_status: 'pending',
      telegram_chat_id: null,
      telegram_message_id: null,
    };
  }

  try {
    const response = await fetch(`https://api.telegram.org/bot${botToken}/sendMessage`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        chat_id: Number(groupChatId),
        text: formatGlobalChatTelegramText(user, message),
        parse_mode: 'HTML',
        disable_web_page_preview: true,
      }),
      signal: AbortSignal.timeout(6000),
    });

    if (!response.ok) {
      logEvent('warn', 'telegram_chat_relay_http_error', {
        request_id: requestId,
        status: response.status,
      });
      return {
        relayed: false,
        relay_status: 'pending',
        telegram_chat_id: groupChatId,
        telegram_message_id: null,
      };
    }

    const payload = (await response.json()) as {
      ok?: boolean;
      result?: { message_id?: number };
    };
    const messageId = payload?.result?.message_id ?? null;

    return {
      relayed: Boolean(payload?.ok && messageId),
      relay_status: payload?.ok && messageId ? 'sent' : 'pending',
      telegram_chat_id: groupChatId,
      telegram_message_id: messageId,
    };
  } catch (err) {
    logEvent('warn', 'telegram_chat_relay_failed', {
      request_id: requestId,
      error: err,
    });
    return {
      relayed: false,
      relay_status: 'pending',
      telegram_chat_id: groupChatId,
      telegram_message_id: null,
    };
  }
}
