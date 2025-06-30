import pandas as pd
import json
import riak

# Вчитување на податоци од CSV датотека
# Како и за Redis, и овде се користат првите 5000 записи од податочното множество за целите на развој, тестирање и споредба.
df = pd.read_csv("IMDB TMDB Movie Metadata Big Dataset (1M).csv", low_memory=False, nrows=5000)

# Отстранување на редови каде 'id' или 'title' се недостасуваат, бидејќи се критични за клучевите.
df = df.dropna(subset=["id", "title"])

# Воспоставување конекција со Riak базата на податоци
# Се користи Protocol Buffers (PBC) протоколот за конекција на порта 8087.
riak_db = riak.RiakClient(pb_port=8087, protocol='pbc')

# Дефинирање на "bucket" (аналог на колекција/табела во други NoSQL бази)
# Сите филмски објекти ќе бидат зачувани во овој bucket.
bucket = riak_db.bucket("movies")

# Итерирање низ DataFrame и вчитување на податоците во Riak
# Секој филм се зачувува како Riak објект, каде клучот е 'movie:ID', а вредноста е JSON репрезентација на филмскиот објект.
for _, row in df.iterrows():
    key = f"movie:{int(row['id'])}"
    value = json.dumps(row.dropna().to_dict())  # Drop NaNs for clean JSON

    # Креирање нов Riak објект со зададениот клуч и податоци
    obj = bucket.new(key, data=value)

    # Зачувување на објектот во Riak
    obj.store()

print("✅ Data loaded into Riak.")

