import asyncio
import io
import logging
import sqlite3
import time
from dataclasses import dataclass
from weakref import WeakValueDictionary

import cv2
import numpy as np
from telegram import Update, Chat, PhotoSize, Message
from telegram.ext import Application, MessageHandler, filters, CommandHandler

import config
from config import conn

logging.basicConfig(format='[%(levelname) 5s/%(asctime)s] %(name)s: %(message)s',
                    level=logging.WARNING)


def dhash_bytes(data: bytes) -> bytes:
    data = np.frombuffer(data, np.uint8)
    img = cv2.imdecode(data, cv2.IMREAD_ANYCOLOR)
    img = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    img = cv2.resize(img, (9, 8), interpolation=cv2.INTER_AREA)
    dhash_bits = np.greater(img[:, :8], img[:, 1:]).flatten()
    dhash = np.packbits(dhash_bits).tobytes()
    return dhash


_mars_cache = WeakValueDictionary()


@dataclass(slots=True, weakref_slot=True)
class MarsInfo:
    group_id: int
    pic_dhash: bytes
    count: int
    last_msg_id: int
    in_whitelist: bool

    @staticmethod
    def query_or_default(cursor: sqlite3.Cursor, group_id: int, dhash: bytes) -> 'MarsInfo':
        tmp = _mars_cache.get((group_id, dhash))
        if tmp is not None:
            return tmp
        start = time.perf_counter_ns()
        row = cursor.execute(
            '''SELECT group_id, pic_dhash, count, last_msg_id, in_whitelist
               FROM mars_info
               WHERE group_id = ?
                 AND pic_dhash = ?''',
            (group_id, dhash)).fetchone()
        end = time.perf_counter_ns()
        print("query one data time elapsed: {} us".format((end - start) / 1000))
        if row is None:
            info = MarsInfo(group_id=group_id, pic_dhash=dhash, count=0, last_msg_id=0, in_whitelist=False)
        else:
            info = MarsInfo(*row)
        _mars_cache[(group_id, dhash)] = info
        return info

    def upsert(self, cursor: sqlite3.Cursor):
        # 存在就更新count和last_msg_id，不存在就新建一个
        start = time.perf_counter_ns()

        cursor.execute(
            '''
            INSERT INTO mars_info (group_id, pic_dhash, count, last_msg_id, in_whitelist)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(group_id, pic_dhash) DO UPDATE SET count=excluded.count,
                                                           last_msg_id=excluded.last_msg_id,
                                                           in_whitelist=excluded.in_whitelist
            ''',
            (self.group_id, self.pic_dhash, self.count, self.last_msg_id, int(self.in_whitelist))
        )
        end = time.perf_counter_ns()
        print("upsert one data time elapsed: {} us".format((end - start) / 1000))
        cursor.connection.commit()


def get_dhash_from_fuid(cursor: sqlite3.Cursor, fuid: str) -> bytes | None:
    row = cursor.execute('SELECT dhash FROM fuid_to_dhash WHERE fuid=?', (fuid,)).fetchone()
    if row is None:
        return None
    return row[0]


def is_user_in_whitelist(cursor: sqlite3.Cursor, group_id: int, user_id: int) -> bool:
    return bool(cursor.execute('SELECT EXISTS (SELECT 1 FROM group_user_in_whitelist WHERE group_id=? AND user_id=?)',
                               (group_id, user_id)))


async def get_dhash(cursor, bot, photo: PhotoSize):
    dhash = get_dhash_from_fuid(cursor, photo.file_unique_id)
    if dhash:
        return dhash
    file = await bot.get_file(photo)
    buf = io.BytesIO()
    await file.download_to_memory(buf)
    dhash = dhash_bytes(buf.getvalue())
    cursor.execute('''INSERT INTO fuid_to_dhash (fuid, dhash)
                      VALUES (?, ?)''', (photo.file_unique_id, dhash))
    cursor.connection.commit()
    return dhash


def get_label(chat: Chat, mars_info: MarsInfo) -> tuple[str, str]:
    if chat.link:
        link = f"{chat.link}/{mars_info.last_msg_id}"
    elif chat.id < 0:
        cid = -chat.id - 1000000000000
        link = f'https://t.me/c/{cid}/{mars_info.last_msg_id}'
    else:
        link = ''
    if link:
        label_start = f'<a href="{link}">'
        label_end = f'</a>'
    else:
        label_start = ''
        label_end = ''
    return label_start, label_end


def build_mars_reply(chat: Chat, mars_info: MarsInfo) -> str:
    label_start, label_end = get_label(chat, mars_info)
    if mars_info.count < 3:
        return f'这张图片已经{label_start}火星{mars_info.count}次{label_end}了！'
    elif mars_info.count == 3:
        return f'这张图已经{label_start}火星了{mars_info.count}次{label_end}了，现在本车送你 ”火星之王“ 称号！'
    else:
        return f'火星之王，收了你的神通吧，这张图都让您{label_start}火星{mars_info.count}次{label_end}了！'


def build_mars_reply_grouped(chat: Chat, mars_info: MarsInfo) -> str:
    label_start, label_end = get_label(chat, mars_info)
    if mars_info.count < 3:
        return f'这一组图片火星了{label_start}火星{mars_info.count}次{label_end}了！'
    elif mars_info.count == 3:
        return f'您这一组图片已经{label_start}火星了{mars_info.count}次{label_end}了，现在本车送你 ”火星之王“ 称号！'
    else:
        return f'火星之王，收了你的神通吧，这些图都让您{label_start}火星{mars_info.count}次{label_end}了！'


_grouped_media: dict[str, list[Message]] = {}


async def msg_queue(msg_list: list[Message]):
    length = len(msg_list)
    for i in range(5):
        await asyncio.sleep(1.5)
        if len(msg_list) == length:
            break
        length = len(msg_list)
    msg = msg_list[0]
    try:
        dhash_list = []
        for m in msg_list:
            if not m.photo:
                continue
            dhash_list.append(
                await get_dhash(conn.cursor(), msg.get_bot(), m.photo[-1])
            )
        for i, dhash in enumerate(dhash_list):
            mars_info = MarsInfo.query_or_default(conn.cursor(), msg.chat_id, dhash)
            msg = msg_list[i]
            if not mars_info.in_whitelist and mars_info.count > 0:
                await msg.reply_html(build_mars_reply_grouped(msg.chat, mars_info), reply_to_message_id=msg.message_id)
                mars_info.count += 1
                mars_info.last_msg_id = msg_list[i].id
                mars_info.upsert(conn.cursor())
                break
            mars_info.count += 1
            mars_info.last_msg_id = msg.chat_id
            mars_info.upsert(conn.cursor())
    finally:
        del _grouped_media[msg.media_group_id]


async def reply_grouped_photo(msg: Message):
    if msg.media_group_id in _grouped_media:
        _grouped_media[msg.media_group_id].append(msg)
        return
    msg_list = [msg]
    _grouped_media[msg.media_group_id] = msg_list
    asyncio.create_task(msg_queue(msg_list), name=f"msg_queue[{msg.media_group_id}]")


async def reply_one_photo(msg: Message):
    chat_id = msg.chat_id
    dhash = await get_dhash(conn.cursor(), msg.get_bot(), msg.photo[-1])
    mars_info = MarsInfo.query_or_default(conn.cursor(), chat_id, dhash)
    if not mars_info.in_whitelist and mars_info.count > 0:
        await msg.reply_html(build_mars_reply(msg.chat, mars_info), reply_to_message_id=msg.message_id)
    mars_info.count += 1
    mars_info.last_msg_id = msg.message_id
    mars_info.upsert(conn.cursor())


async def reply_photo(update: Update, _ctx):
    if is_user_in_whitelist(conn.cursor(), update.effective_chat.id, update.effective_user.id):
        return
    if update.message.media_group_id:
        await reply_grouped_photo(update.message)
    else:
        await reply_one_photo(update.message)


async def get_refer_photo(update: Update):
    photo = None
    if update.message.photo:
        photo = update.message.photo[-1]
    elif update.message.reply_to_message and update.message.reply_to_message.photo:
        photo = update.message.reply_to_message.photo[-1]
    if not photo:
        await update.message.reply_text('火星车没有发现您引用了任何图片。\n尝试发送图片使用命令，或回复特定图片。',
                                        reply_to_message_id=update.message.message_id)
        return None
    return photo


async def get_pic_info(update: Update, _ctx):
    photo = await get_refer_photo(update)
    if not photo:
        return
    dhash = await get_dhash(conn.cursor(), update.get_bot(), photo)
    mars_info = MarsInfo.query_or_default(conn.cursor(), update.message.chat_id, dhash)
    whitelist_str = '🙈 它在本群的火星白名单中' if mars_info.in_whitelist else '🟢 它不在本群的火星白名单当中'
    await update.message.reply_text(f'File unique id: {photo.file_unique_id}\n'
                                    f'dhash: {dhash.hex().upper()}\n'
                                    f'在本群的火星次数:{mars_info.count}\n'
                                    f'{whitelist_str}',
                                    reply_to_message_id=update.message.message_id)


async def add_to_whitelist(update: Update, _ctx):
    photo = await get_refer_photo(update)
    if not photo:
        return
    dhash = await get_dhash(conn.cursor(), update.get_bot(), photo)
    mars_info = MarsInfo.query_or_default(conn.cursor(), update.message.chat_id, dhash)
    if mars_info.in_whitelist:
        await update.message.reply_text('这张图片已经在白名单当中了', reply_to_message_id=update.message.message_id)
        return
    mars_info.in_whitelist = True
    mars_info.upsert(conn.cursor())
    await update.message.reply_text('成功将图片加入白名单', reply_to_message_id=update.message.message_id)


async def remove_from_whitelist(update: Update, _ctx):
    photo = await get_refer_photo(update)
    if not photo:
        await update.message.reply_text('火星车没有发现您引用了任何图片。\n尝试发送图片使用命令，或回复特定图片。',
                                        reply_to_message_id=update.message.message_id)
        return
    dhash = await get_dhash(conn.cursor(), update.get_bot(), photo)
    mars_info = MarsInfo.query_or_default(conn.cursor(), update.message.chat_id, dhash)
    if mars_info.in_whitelist:
        await update.message.reply_text('这张图片并不在白名单中', reply_to_message_id=update.message.message_id)
        return
    mars_info.in_whitelist = False
    mars_info.upsert(conn.cursor())
    await update.message.reply_text('成功将图片移除白名单', reply_to_message_id=update.message.message_id)


async def bot_help(update: Update, _ctx):
    bot_name = update.get_bot().username
    at_suffix = f'@{bot_name}'
    if update.message.chat.type == 'private':
        at_suffix = ''

    await update.message.reply_text(
        f'/help{at_suffix} 显示本帮助信息\n'
        f'/pic_info{at_suffix} 获取图片信息\n'
        f'/add_whitelist{at_suffix} 将图片添加到白名单\n'
        f'/remove_from_whitelist{at_suffix} 将图片移除白名单')


def main():
    application = (Application.builder()
                   .proxy("http://localhost:7451")
                   .token(config.BOT_TOKEN).build())
    application.add_handler(MessageHandler(filters.PHOTO, reply_photo))
    application.add_handler(CommandHandler("pic_info", get_pic_info))
    application.add_handler(CommandHandler("add_whitelist", add_to_whitelist))
    application.add_handler(CommandHandler("remove_whitelist", remove_from_whitelist))
    application.add_handler(CommandHandler("help", bot_help))
    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == '__main__':
    try:
        main()
    finally:
        conn.commit()
