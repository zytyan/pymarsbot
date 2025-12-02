import asyncio
import io
import os
import sqlite3
import subprocess
import threading
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timedelta
from weakref import WeakValueDictionary

import cv2
import httpx
import numpy as np
from telegram import Update, Chat, PhotoSize, Message, InlineKeyboardButton, InlineKeyboardMarkup, ChatMember
from telegram.ext import Application, MessageHandler, filters, CommandHandler, CallbackQueryHandler, ChatMemberHandler


def hamming_distance(a: bytes, b: bytes) -> int:
    x = int.from_bytes(a, "big")
    y = int.from_bytes(b, "big")
    n = (x ^ y).bit_count()
    return n


def try_build_hamm_acc():
    outfile = "hammdist.so"
    cfile = "hammdist.c"
    if os.path.isfile(outfile) and os.path.getmtime(outfile) > os.path.getmtime(cfile):
        # so文件存在，且比C文件新，那么就不需要编译，否则编译一下
        return
    # gcc -O3 -march=native -fPIC -shared hamdist_opt.c -o hamdist_opt.so
    subprocess.run(["gcc", "-O3", "-march=native", "-fPIC", "-shared", cfile, "-o", outfile], check=True)
    if not os.path.isfile(outfile):
        raise FileNotFoundError(f"{outfile} not found")


def init_database(connection: sqlite3.Connection):
    cursor = connection.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS mars_info
                      (
                          group_id     INTEGER NOT NULL,
                          pic_dhash    BLOB    NOT NULL,
                          count        INTEGER NOT NULL DEFAULT 0 CHECK (count >= 0),
                          last_msg_id  INTEGER NOT NULL DEFAULT 0 CHECK (last_msg_id >= 0),
                          in_whitelist INTEGER NOT NULL DEFAULT 0 CHECK (in_whitelist IN (0, 1)),
                          PRIMARY KEY (group_id, pic_dhash)
                      ) WITHOUT ROWID;''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS fuid_to_dhash
                      (
                          fuid  TEXT PRIMARY KEY NOT NULL,
                          dhash BLOB             NOT NULL
                      ) WITHOUT ROWID;''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS group_user_in_whitelist
                      (
                          group_id INTEGER NOT NULL,
                          user_id  INTEGER NOT NULL,
                          PRIMARY KEY (group_id, user_id)
                      ) WITHOUT ROWID;''')
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA synchronous=OFF")
    cursor.execute("PRAGMA cache_size=-80000;")
    try:
        try_build_hamm_acc()
        conn.enable_load_extension(True)
        # 加载你的 hamdist.so
        conn.load_extension("./hammdist.so")
    except Exception as e:
        print(f"遇到错误，使用python内建函数，错误: {e}")
        connection.create_function('hamming_distance', 2, hamming_distance)
    connection.commit()


conn = sqlite3.connect('mars.db', check_same_thread=False)
init_database(conn)


def backup_database():
    print("Backing up database...")
    filename = 'backup_mars_at_{}.db'.format(time.strftime("%Y-%m-%d-%H_%M_%S"))
    backup = sqlite3.connect(filename)
    with backup:
        conn.backup(backup)
    backup.close()
    import zstd
    zstd_filename = filename + '.zstd'
    with open(filename, 'rb') as fi, open(zstd_filename, 'wb') as fo:
        # noinspection PyTypeChecker
        fo.write(zstd.compress(fi.read(), 5, max(1, os.cpu_count() - 2)))
    os.remove(filename)
    s3_api = os.getenv("S3_API_ENDPOINT")
    if not s3_api:
        print("没有配置 S3_API_ENDPOINT ，仅将文件备份在本地。")
        return
    key_id = os.getenv("S3_API_KEY_ID")
    if not key_id:
        print("配置了S3存储用于备份，但没有提供key id，无法上传，请确认您配置了环境变量 S3_API_KEY_ID")
        return
    key_secret = os.getenv("S3_API_KEY_SECRET")
    if not key_secret:
        print("配置了S3存储用于备份，但没有提供secret key，无法上传，请确认您配置了环境变量 S3_API_KEY_SECRET")
        return
    bucket = os.getenv("S3_BUCKET")
    if not bucket:
        print("没有配置 S3_BUCKET, 程序无法确定使用哪个存储桶")
        return
    import boto3
    print(f"开始向S3备份，API={s3_api}, key={key_id}, secret={key_secret[:2]}***{key_secret[-2:]}")
    s3 = boto3.client(
        "s3",
        endpoint_url=s3_api,
        aws_access_key_id=key_id,
        aws_secret_access_key=key_secret,
    )
    s3.upload_file(zstd_filename, bucket, zstd_filename)
    os.remove(zstd_filename)


def start_backup_thread():
    if os.getenv("NO_BACKUP"):
        print("检测到 NO_BACKUP 环境变量，不备份数据库")
        return
    print("通过配置 NO_BACKUP 环境环境变量避免备份数据库")
    try:
        interval_minutes = float(os.getenv("BACKUP_INTERVAL_MINUTES"))
    except (ValueError, TypeError):
        print("未配置备份间隔环境变量 BACKUP_INTERVAL_MINUTES 或备份间隔解析失败，使用默认间隔（12小时）")
        interval_minutes = 720

    def inner():
        while True:
            try:
                with open("last_backup_time.txt", "a+") as f:
                    f.seek(0)
                    time_format = "%Y-%m-%d %H:%M:%S"
                    try:
                        last = datetime.strptime(f.read(), time_format)
                    except ValueError:
                        last = datetime.fromtimestamp(0)
                    if datetime.now() - last > timedelta(minutes=interval_minutes):
                        backup_database()  # 若失败应该会抛exception
                        f.seek(0)
                        f.write(datetime.now().strftime(time_format))
                        f.truncate()
                time.sleep(interval_minutes * 60)
            except KeyboardInterrupt:
                return
            except Exception as e:
                print(e)
                # 出现错误十分钟后重试
                time.sleep(600)

    thread = threading.Thread(target=inner, daemon=True, name="db_backup_thread")
    thread.start()


def dhash_bytes(data: bytes) -> bytes:
    data = np.frombuffer(data, np.uint8)
    img = cv2.imdecode(data, cv2.IMREAD_ANYCOLOR)
    img = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    img = cv2.resize(img, (9, 8), interpolation=cv2.INTER_AREA)
    dhash_bits = np.greater(img[:, :8], img[:, 1:]).flatten()
    dhash = np.packbits(dhash_bits).tobytes()
    return dhash


# 这个是用来维护跨await时的数据库一致性的，不是缓存，不能删
# 虽然数据库更新是原子的，但是代码不是
_mars_info_weak_ref = WeakValueDictionary()


@dataclass(slots=True, weakref_slot=True)
class MarsInfo:
    group_id: int
    pic_dhash: bytes
    count: int
    last_msg_id: int
    in_whitelist: bool

    @staticmethod
    def query_or_default(cursor: sqlite3.Cursor, group_id: int, dhash: bytes) -> 'MarsInfo':
        if cache := _mars_info_weak_ref.get((group_id, dhash), None):
            return cache
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
        _mars_info_weak_ref[(group_id, dhash)] = info
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

    @staticmethod
    def find_similar(cursor: sqlite3.Cursor, group_id: int, dhash: bytes, threshold) -> list['MarsInfo']:
        # 无论是find_similar还是clone都不需要放到一致性map里，它们不需要什么一致性
        rows = cursor.execute('''
                              SELECT group_id, pic_dhash, count, last_msg_id, in_whitelist
                              FROM (SELECT group_id,
                                           pic_dhash,
                                           count,
                                           last_msg_id,
                                           in_whitelist,
                                           hamming_distance(pic_dhash, ?) AS hd
                                    FROM mars_info
                                    WHERE group_id = ?)
                              WHERE hd < ?
                              ORDER BY hd
                              LIMIT 10''', (dhash, group_id, threshold))
        return [MarsInfo(*row) for row in rows]

    def clone(self) -> 'MarsInfo':
        return MarsInfo(self.group_id, self.pic_dhash, self.count, self.last_msg_id, self.in_whitelist)


def get_dhash_from_fuid(cursor: sqlite3.Cursor, fuid: str) -> bytes | None:
    row = cursor.execute('SELECT dhash FROM fuid_to_dhash WHERE fuid=?', (fuid,)).fetchone()
    if row is None:
        return None
    return row[0]


def is_user_in_whitelist(cursor: sqlite3.Cursor, group_id: int, user_id: int) -> bool:
    return bool(cursor.execute('SELECT EXISTS (SELECT 1 FROM group_user_in_whitelist WHERE group_id=? AND user_id=?)',
                               (group_id, user_id)).fetchone()[0])


_report_stat_url = os.getenv('REPORT_STAT_URL')
_report_http_client = None
if _report_stat_url:
    _report_http_client = httpx.AsyncClient()


async def report_to_stat(group_id, mars_count):
    if not _report_stat_url:
        return
    await _report_http_client.post(_report_stat_url, json={'group_id': group_id, 'mars_count': mars_count})


async def get_dhash(cursor, bot, photo: PhotoSize):
    dhash = get_dhash_from_fuid(cursor, photo.file_unique_id)
    if dhash:
        return dhash
    file = await bot.get_file(photo)
    buf = io.BytesIO()
    await file.download_to_memory(buf)
    dhash = dhash_bytes(buf.getvalue())
    cursor.execute('''INSERT INTO fuid_to_dhash (fuid, dhash)
                      VALUES (?, ?)
                      ON CONFLICT DO NOTHING ''', (photo.file_unique_id, dhash))
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


_grouped_media: dict[str, asyncio.Queue[Message]] = {}


async def grouped_media_proc(msg_queue: asyncio.Queue[Message]) -> None:
    msg_list = []
    for i in range(10):
        try:
            msg = await asyncio.wait_for(msg_queue.get(), timeout=1.5)
            msg_list.append(msg)
        except asyncio.TimeoutError:
            break
    dhash_list = []
    for msg in msg_list:
        if not msg.photo:
            continue
        dhash_list.append(
            await get_dhash(conn.cursor(), msg.get_bot(), msg.photo[-1])
        )
    final_mars_info: MarsInfo | None = None
    final_msg: Message | None = None
    for msg, dhash in zip(msg_list, dhash_list, strict=True):
        mars_info = MarsInfo.query_or_default(conn.cursor(), msg.chat_id, dhash)
        if mars_info.in_whitelist:
            continue
        if mars_info.count > 0:
            asyncio.create_task(report_to_stat(msg.chat_id, mars_info.count)).add_done_callback(async_task_done)
            if final_mars_info is None or mars_info.count > final_mars_info.count:
                final_mars_info = mars_info.clone()
                final_msg = msg
        mars_info.count += 1
        mars_info.last_msg_id = msg.id
        mars_info.upsert(conn.cursor())
    if final_mars_info:
        await final_msg.reply_html(build_mars_reply_grouped(final_msg.chat, final_mars_info),
                                   reply_to_message_id=final_msg.message_id)


async def reply_grouped_photo(msg: Message):
    if msg.media_group_id in _grouped_media:
        _grouped_media[msg.media_group_id].put_nowait(msg)
        return
    msg_queue = asyncio.Queue(maxsize=10)  # telegram 最多支持10个Photo消息，那我做10个总归是不会有问题
    msg_queue.put_nowait(msg)
    _grouped_media[msg.media_group_id] = msg_queue
    task = asyncio.create_task(grouped_media_proc(msg_queue),
                               name=f"grouped_media_proc[{msg.media_group_id}]")
    task.add_done_callback(async_task_done)

    def del_on_end(_t):
        # 无论是否发生异常，都要删掉这个media_group_id，避免发生内存泄漏
        del _grouped_media[msg.media_group_id]

    task.add_done_callback(del_on_end)


async def reply_one_photo(msg: Message):
    chat_id = msg.chat_id
    dhash = await get_dhash(conn.cursor(), msg.get_bot(), msg.photo[-1])
    mars_info = MarsInfo.query_or_default(conn.cursor(), chat_id, dhash)
    if mars_info.last_msg_id == msg.id:
        return
    if mars_info.in_whitelist:
        return
    if mars_info.count > 0:
        reply_markup = None
        if mars_info.count > 5:
            reply_markup = InlineKeyboardMarkup(
                [[InlineKeyboardButton("将图片添加至白名单", callback_data=f'wl:{mars_info.pic_dhash.hex()}'), ]]
            )
        asyncio.create_task(report_to_stat(msg.chat_id, mars_info.count),
                            name=f"reply_to_stat[{mars_info.count}]").add_done_callback(async_task_done)
        await msg.reply_html(build_mars_reply(msg.chat, mars_info),
                             reply_to_message_id=msg.message_id,
                             reply_markup=reply_markup)
    mars_info.count += 1
    mars_info.last_msg_id = msg.message_id
    mars_info.upsert(conn.cursor())


async def reply_photo(update: Update, _ctx):
    chat = update.effective_chat
    user = update.effective_user
    if is_user_in_whitelist(conn.cursor(), update.effective_chat.id, update.effective_user.id):
        print(f"user {chat.effective_name}({chat.id})/{user.full_name}({user.id}) 在白名单中，忽略")
        return
    print(f"尝试处理含图片消息 {chat.effective_name}({chat.id})/{user.full_name}({user.id})")
    if update.effective_message.media_group_id:
        if not update.edited_message:
            await reply_grouped_photo(update.effective_message)
    else:
        await reply_one_photo(update.effective_message)


async def get_refer_photo(update: Update):
    photo = None
    if update.effective_message.photo:
        photo = update.effective_message.photo[-1]
    elif update.effective_message.reply_to_message and update.effective_message.reply_to_message.photo:
        photo = update.effective_message.reply_to_message.photo[-1]
    if not photo:
        await update.effective_message.reply_text(
            '火星车没有发现您引用了任何图片。\n尝试发送图片使用命令，或回复特定图片。',
            reply_to_message_id=update.effective_message.message_id)
        return None
    return photo


async def add_pic_whitelist_by_cb(update: Update, _ctx):
    group_id = update.callback_query.message.chat.id
    dhash = bytes.fromhex(update.callback_query.data.split(":")[1])
    mars_info = MarsInfo.query_or_default(conn.cursor(), group_id, dhash)
    mars_info.in_whitelist = True
    mars_info.upsert(conn.cursor())
    await update.callback_query.answer("该图片已加入白名单")


async def get_pic_info(update: Update, _ctx):
    photo = await get_refer_photo(update)
    if not photo:
        return
    dhash = await get_dhash(conn.cursor(), update.get_bot(), photo)
    mars_info = MarsInfo.query_or_default(conn.cursor(), update.effective_message.chat_id, dhash)
    whitelist_str = '🙈 它在本群的火星白名单中' if mars_info.in_whitelist else '🟢 它不在本群的火星白名单当中'
    reply_markup = InlineKeyboardMarkup([[
        InlineKeyboardButton("查找DHASH相似图片", callback_data=f'find:{mars_info.pic_dhash.hex()}'),
    ]])
    await update.effective_message.reply_text(f'File unique id: {photo.file_unique_id}\n'
                                              f'dhash: {dhash.hex().upper()}\n'
                                              f'在本群的火星次数:{mars_info.count}\n'
                                              f'{whitelist_str}',
                                              reply_to_message_id=update.effective_message.message_id,
                                              reply_markup=reply_markup)


async def add_to_whitelist(update: Update, _ctx):
    photo = await get_refer_photo(update)
    if not photo:
        return
    dhash = await get_dhash(conn.cursor(), update.get_bot(), photo)
    mars_info = MarsInfo.query_or_default(conn.cursor(), update.effective_message.chat_id, dhash)
    if mars_info.in_whitelist:
        await update.effective_message.reply_text('这张图片已经在白名单当中了',
                                                  reply_to_message_id=update.effective_message.message_id)
        return
    mars_info.in_whitelist = True
    mars_info.upsert(conn.cursor())
    await update.effective_message.reply_text('成功将图片加入白名单',
                                              reply_to_message_id=update.effective_message.message_id)


async def remove_from_whitelist(update: Update, _ctx):
    photo = await get_refer_photo(update)
    if not photo:
        return
    dhash = await get_dhash(conn.cursor(), update.get_bot(), photo)
    mars_info = MarsInfo.query_or_default(conn.cursor(), update.effective_message.chat_id, dhash)
    if not mars_info.in_whitelist:
        await update.effective_message.reply_text('这张图片并不在白名单中',
                                                  reply_to_message_id=update.effective_message.message_id)
        return
    mars_info.in_whitelist = False
    mars_info.upsert(conn.cursor())
    await update.effective_message.reply_text('成功将图片移除白名单',
                                              reply_to_message_id=update.effective_message.message_id)


def bot_stat_inner(update: Update):
    msg = update.effective_message
    user = update.effective_user
    start = time.perf_counter_ns()
    group_count = conn.execute('SELECT COUNT(DISTINCT group_id) FROM mars_info WHERE group_id < 0').fetchone()[0]
    mars_count = conn.execute('SELECT COUNT(pic_dhash) FROM mars_info WHERE group_id=?',
                              (msg.chat_id,)).fetchone()[0]
    exists = '在' if is_user_in_whitelist(conn.cursor(), msg.chat.id, user.id) else '不在'
    end = time.perf_counter_ns()
    return (f'火星车当前一共服务了{group_count}个群组\n'
            f'当前群组ID: {msg.chat_id}\n'
            f'您是 {user.full_name}(id:{user.id})，您{exists}本群的白名单当中\n'
            f'本群一共记录了 {mars_count} 张不同的图片\n'
            f'本次统计共耗时 {(end - start) / 1_000_000:.2f} ms\n'
            f'火星车与您同在')


async def bot_stat(update: Update, _ctx):
    text = await asyncio.to_thread(bot_stat_inner, update)
    await update.effective_message.reply_text(text)


async def bot_help(update: Update, _ctx):
    if update.effective_chat.type != Chat.PRIVATE and update.message.text.startswith('/start'):
        # 不响应群组中的start命令
        return
    bot_name = update.get_bot().username
    at_suffix = f'@{bot_name}'
    if update.effective_message.chat.type == 'private':
        at_suffix = ''

    await update.effective_message.reply_text(
        f'/help{at_suffix} 显示本帮助信息\n'
        f'/stat{at_suffix} 显示统计信息\n'
        f'/pic_info{at_suffix} 获取图片信息\n'
        f'/add_whitelist{at_suffix} 将图片添加到白名单\n'
        f'/remove_from_whitelist{at_suffix} 将图片移除白名单\n'
        f'/add_me_to_whitelist{at_suffix} 将用户加入群组白名单\n'
        f'/remove_me_from_whitelist{at_suffix} 将用户移出群组白名单\n'
        f'/export{at_suffix} 导出本聊天中火星车的数据')


async def send_welcome(bot, chat_id):
    await bot.send_message(
        chat_id,
        '欢迎使用火星车。\n'
        '本bot为 @Ytyan 为其群组开发的重复图片检测工具\n'
        '当您将火星车加入群组或频道中后，火星车将自动开始工作。bot会实时检测群组中的图片，将其转换为DHASH，当检测到重复图片时，会回复图片的发送者。\n'
        'bot会收集并持久保存工作需要的必要信息，包括群组ID、图片唯一ID、图片DHASH和携带图片的消息的ID。bot会在必要时下载图片，但不会持久保存\n'
        'bot只会检查普通图片，文件形式的图片、表情包、视频等均不会被检测。\n'
        '本bot为开源项目，您可以前往<a href="https://github.com/zytyan/pymarsbot">Github开源地址</a>自行克隆该项目。',
        parse_mode='HTML')


async def welcome(update: Update, _ctx):
    print(update)
    member = update.my_chat_member
    if update.effective_chat.type == Chat.PRIVATE:
        return
    if (update.effective_chat.type in (Chat.GROUP, Chat.SUPERGROUP) and
            member.new_chat_member.status == ChatMember.ADMINISTRATOR):
        await member.get_bot().send_message(update.effective_chat.id,
                                            '火星车的任何功能均不需要管理员权限，您无需将本bot设置为群组管理员。')
        return
    if member.old_chat_member.status not in (ChatMember.LEFT, ChatMember.BANNED):
        return
    if member.new_chat_member.status == ChatMember.MEMBER:
        await send_welcome(update.get_bot(), update.effective_chat.id)


async def cmd_welcome(update: Update, _ctx):
    await send_welcome(update.get_bot(), update.effective_chat.id)


def export(chat_id: int):
    rows = conn.cursor().execute(
        '''SELECT group_id, pic_dhash, count, last_msg_id, in_whitelist
           FROM mars_info
           WHERE group_id = ?''', (chat_id,))
    filename = f'mars-export_{chat_id}.csv'
    with open(filename, 'w') as f:
        f.write('group_id,pic_dhash,count,last_msg_id,in_whitelist\n')
        for row in rows:
            f.write("{},{},{},{},{}\n".format(row[0], row[1].hex(), row[2], row[3], row[4]))
    return filename


@dataclass
class ExportingChat:
    chat_id: int
    time: float
    running: bool = False


_exporting_chat: dict[int, ExportingChat] = {}


def async_task_done(t: asyncio.Task):
    print(f"task {t.get_name()} done")
    exc = t.exception()
    if exc is not None:
        print(f"task {t.get_name()} : exception {exc}")
        # 打印完整的异常栈
        tb = exc.__traceback__
        traceback.print_exception(type(exc), exc, tb)


async def export_data(update: Update, _ctx):
    chat_id = update.effective_chat.id
    if exporting := _exporting_chat.get(chat_id):
        if exporting.running:
            await update.effective_message.reply_text('当前正在导出数据，请稍候再试')
            return
        await update.effective_message.reply_text('请不要短时间内重复导出，每次单个群组导出冷却时间为10分钟。')
        return

    async def delete_exporting():
        await asyncio.sleep(10 * 60)
        print(f"delete exporting chat id={chat_id}")
        _exporting_chat.pop(chat_id, None)

    _exporting_chat[chat_id] = ExportingChat(chat_id, time.time(), True)
    filename = await asyncio.to_thread(export, chat_id)
    out_filename = filename
    try:
        tar_filename = f"{filename}.tar.gz"
        proc = await asyncio.create_subprocess_exec("tar", "-zcf", tar_filename, out_filename)
        result = await proc.wait()
        if result == 0:
            os.remove(out_filename)
            out_filename = tar_filename
    except FileNotFoundError:
        pass
    try:
        await update.effective_message.reply_document(out_filename)
    except Exception as e:
        print(e)
        _exporting_chat.pop(chat_id, None)
        await update.effective_message.reply_text(f'导出失败，错误: {e}')
    finally:
        _exporting_chat[chat_id].running = False
        os.remove(out_filename)
    task = asyncio.create_task(delete_exporting(), name=f'export {chat_id} mars info')
    task.add_done_callback(async_task_done)


async def export_help(update: Update, _ctx):
    await update.message.reply_text('想部署自己的火星车，又放不下当前数据？\n'
                                    '现在，您可以使用命令 /ensure_marsbot_export 导出火星车的数据，它们包括'
                                    '群组ID、DHASH值、火星数量、上一次消息ID及白名单状态\n'
                                    '这些信息将会被导出为tar压缩的csv格式，您可以在解压后放心地直接使用逗号分割。\n'
                                    '请注意，为避免无意义的性能消耗，每个群组在十分钟内只能导出一次。')


async def add_user_to_whitelist(update: Update, _ctx):
    group_id = update.effective_chat.id
    user = update.effective_user
    try:
        conn.execute('''INSERT INTO group_user_in_whitelist(group_id, user_id)
                        VALUES (?, ?)''', (group_id, user.id))
        await update.effective_message.reply_text(f'已将用户 {user.full_name} 加入白名单，您发的任何图片都不会被处理。')
    except sqlite3.IntegrityError:
        await update.effective_message.reply_text(
            f'用户 {user.full_name} 已经在本群的白名单中，您发的任何图片都不会被处理。')


async def remove_user_from_whitelist(update: Update, _ctx):
    group_id = update.effective_chat.id
    user = update.effective_user
    cur = conn.cursor()
    cur.execute('''DELETE
                   FROM group_user_in_whitelist
                   WHERE group_id = ?
                     AND user_id = ?''', (group_id, user.id))
    if cur.rowcount == 0:
        await update.effective_message.reply_text(f'用户 {user.full_name} 不在本群白名单中，火星车正在工作。')
        return
    await update.effective_message.reply_text(f'已将用户 {user.full_name} 移除本群白名单，火星车会继续为您服务。')


async def find_similar_img_by_cb(update: Update, _ctx):
    start = time.perf_counter_ns()
    chat_id = update.effective_chat.id
    dhash = bytes.fromhex(update.callback_query.data.split(':')[1])

    mars_info_list = await asyncio.to_thread(MarsInfo.find_similar, conn.cursor(), chat_id, dhash, 6)
    end = time.perf_counter_ns()
    head = (f'火星车为您找到了{len(mars_info_list)}张相似的图片\n'
            f'这些图片的汉明距离小于6\n'
            f'耗时:{(end - start) / 1000_000}ms\n')  # 这里保留一个换行符，和下面做出区别，并非出错
    text_buf = [head]
    for i, mars_info in enumerate(mars_info_list):
        label_start, label_end = get_label(update.effective_chat, mars_info)
        text_buf.append(
            f'{label_start}图片{i + 1}: 距离: {hamming_distance(dhash, mars_info.pic_dhash)} 消息ID: {mars_info.last_msg_id}{label_end}'
        )
    await update.effective_message.reply_html('\n'.join(text_buf))
    await update.callback_query.answer(f'查找完成', show_alert=False)


def main():
    builder = Application.builder()
    if not os.getenv("BOT_TOKEN"):
        print("需要配置环境变量 BOT_TOKEN, 请使用 export BOT_TOKEN=<YOUR_BOT_TOKEN> 来配置")
        exit(1)
    builder.token(os.getenv("BOT_TOKEN"))
    if base_url := os.getenv('BOT_BASE_URL'):
        builder.base_url(base_url)
    if base_file_url := os.getenv('BOT_BASE_FILE_URL'):
        builder.base_file_url(base_file_url)
    if proxy := os.getenv('BOT_PROXY'):
        builder.proxy(proxy)
    start_backup_thread()
    application = builder.build()
    application.add_handler(MessageHandler(filters.PHOTO, reply_photo))
    application.add_handler(CallbackQueryHandler(add_pic_whitelist_by_cb, r'^wl:[\da-fA-F]+$'))
    application.add_handler(CallbackQueryHandler(find_similar_img_by_cb, r'^find:[\da-fA-F]+$'))
    application.add_handler(CommandHandler("pic_info", get_pic_info))
    application.add_handler(CommandHandler("add_whitelist", add_to_whitelist))
    application.add_handler(CommandHandler("remove_whitelist", remove_from_whitelist))
    application.add_handler(CommandHandler("add_me_to_whitelist", add_user_to_whitelist))
    application.add_handler(CommandHandler("remove_me_from_whitelist", remove_user_from_whitelist))
    application.add_handler(CommandHandler("help", bot_help))
    application.add_handler(CommandHandler("start", bot_help))
    application.add_handler(CommandHandler("stat", bot_stat))
    application.add_handler(CommandHandler("mars_bot_welcome", cmd_welcome))
    application.add_handler(CommandHandler("ensure_marsbot_export", export_data))
    application.add_handler(CommandHandler("export", export_help))

    application.add_handler(ChatMemberHandler(welcome))
    application.run_polling(
        allowed_updates=[
            # 用于处理bot的按钮
            Update.CALLBACK_QUERY,
            # 处理群组消息
            Update.CHANNEL_POST, Update.MESSAGE, Update.EDITED_MESSAGE,
            # 将来bot被加入到群组时可以回应
            Update.MY_CHAT_MEMBER
        ], drop_pending_updates=False)


if __name__ == '__main__':
    try:
        main()
    finally:
        conn.commit()
