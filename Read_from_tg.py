import datetime
import time
from threading import Thread
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from telethon import TelegramClient, events

time_to_reset = 60  # время сброса в файл новых данных

api_id = 1 # Свое
api_hash = 1 # Свое

client = TelegramClient('user', api_id, api_hash).start()


def get_nikname(channel_obj, list_obj, list_channel):
    # находит канал из которого пишло сообщение из списка каналов за которыми следим
    for i in range(len(list_obj)):
        if list_obj[i].id == channel_obj.channel_id:
            return list_channel[i]
    return None


def media_exist(mes):
    # Проверка наличия медиа в сообщении
    if (mes.photo is not None
        or mes.document is not None
        or mes.web_preview is not None
        or mes.voice is not None
        or mes.audio is not None
        or mes.video is not None
        or mes.video is not None
        or mes.video_note is not None
        or mes.gif is not None
    ):
        return "yes"
    else:
        return "no"


def drop_data(mes):
    # Основная функция пишушая получаемые данные в файл parquet
    prev = str(datetime.datetime.now().strftime("%Y-%m-%d %H-%M-%S"))
    while True:
        time.sleep(time_to_reset)
        df = pd.DataFrame.from_dict(mes)
        table = pa.Table.from_pandas(df)
        cur = str(datetime.datetime.now().strftime("%Y-%m-%d %H-%M-%S"))
        name_of_database = 'mes_data ' + 'from ' + prev + ' to ' + cur + '.parquet'
        pq.write_table(table, name_of_database)
        prev = cur
        mes['text'] = []
        mes['channel'] = []
        mes['date'] = []
        mes['Is Media Exist'] = []
        mes['time'] = []
        print(pq.read_table(name_of_database).to_pandas())


async def main():
    # Аутентификация клиента
    await client.start()

    mes = {'text': [],
           'channel': [],
           'date': [],
           'Is Media Exist': [],
           'time': []
           }

    # каналы
    channel1 = ['@rian_ru', '@breakingmash', '@leoday', '@spark_session_test', '@novosti_voinaa', '@spark_session_test']

    channel_1 = await client.get_entity(channel1)

    @client.on(events.NewMessage(chats=channel_1))
    async def new_message_listener(event):
        # Обработка нового сообщения
        mes['text'].append(event.message.text)
        mes['channel'].append(get_nikname(event.message.peer_id, channel_1, channel1))
        mes['date'].append(str(datetime.datetime.now()))
        mes['Is Media Exist'].append(media_exist(event.message))
        mes['time'].append(str(datetime.datetime.now().timestamp()))

    print("Service is started")
    thread2 = Thread(target=drop_data, args=(mes, ))
    thread2.start()
    await client.run_until_disconnected()

with client:
    client.loop.run_until_complete(main())
