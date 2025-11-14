import sqlite3
import os

if not os.environ.get("BOT_TOKEN"):
    print("需要配置环境变量 BOT_TOKEN, 请使用 export BOT_TOKEN=<YOUR_BOT_TOKEN> 来配置")
    exit(1)

BOT_TOKEN: str = os.environ['BOT_TOKEN']

def init(connection: sqlite3.Connection):
    cursor = connection.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS mars_info
                      (
                          group_id     INTEGER NOT NULL,
                          pic_dhash    BLOB    NOT NULL,
                          count        INTEGER NOT NULL DEFAULT 0 CHECK (count > 0),
                          last_msg_id  INTEGER NOT NULL DEFAULT 0 CHECK (last_msg_id > 0),
                          in_whitelist INTEGER NOT NULL DEFAULT 0 CHECK (in_whitelist IN (0, 1)),
                          UNIQUE (group_id, pic_dhash)
                      );''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS fuid_to_dhash
                      (
                          fuid  TEXT    UNIQUE NOT NULL ,
                          dhash BLOB    NOT NULL
                      )''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS group_user_in_whitelist
                (
                    group_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    PRIMARY KEY (group_id, user_id)
                ) WITHOUT ROWID;''')
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA synchronous=OFF")
    cursor.execute("PRAGMA cache_size=-20000;")
    connection.commit()

conn = sqlite3.connect('mars.db')
init(conn)