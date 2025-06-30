import riak
import time
import json

riak_db = riak.RiakClient(pb_port=8087, protocol='pbc')

movie_bucket = riak_db.bucket('movies')
genre_bucket = riak_db.bucket('genre')
actor_bucket = riak_db.bucket('actor')
year_bucket = riak_db.bucket('year')
top_rated_bucket = riak_db.bucket('top_rated')

def load_title(movie_key):
    if isinstance(movie_key, bytes):
        movie_key = movie_key.decode()
    obj = movie_bucket.get(movie_key)
    if obj.exists:
        try:
            movie = json.loads(obj.data) if isinstance(obj.data, str) else obj.data
            return movie.get("title", "Unknown Title")
        except:
            return "Invalid JSON"
    return "Not Found"

# ----------- Simple Queries -----------

def query_by_genre(genre="Action"):
    start = time.time()
    genre_obj = genre_bucket.get(genre)
    movie_ids = genre_obj.data or []
    titles = [load_title(mid) for mid in movie_ids]
    end = time.time()
    print(f"[Genre: {genre}] Found {len(titles)} movies in {end - start:.4f} seconds")

def query_by_actor(actor="Tom Hanks"):
    start = time.time()
    actor_obj = actor_bucket.get(actor)
    movie_ids = actor_obj.data or []
    titles = [load_title(mid) for mid in movie_ids]
    end = time.time()
    print(f"[Actor: {actor}] Found {len(titles)} movies in {end - start:.4f} seconds")

def query_by_year(year="2015"):
    start = time.time()
    year_obj = year_bucket.get(year)
    movie_ids = year_obj.data or []
    titles = [load_title(mid) for mid in movie_ids]
    end = time.time()
    print(f"[Year: {year}] Found {len(titles)} movies in {end - start:.4f} seconds")

# ----------- Complex Queries -----------

def query_by_actor_and_genre(actor="Tom Hanks", genre="Drama"):
    start = time.time()
    actor_set = set(actor_bucket.get(actor).data or [])
    genre_set = set(genre_bucket.get(genre).data or [])
    movie_ids = actor_set.intersection(genre_set)
    titles = [load_title(mid) for mid in movie_ids]
    end = time.time()
    print(f"[Actor: {actor} AND Genre: {genre}] Found {len(titles)} movies in {end - start:.4f} seconds")

def query_by_genre_and_year(genre="Action", year="2015"):
    start = time.time()
    genre_set = set(genre_bucket.get(genre).data or [])
    year_set = set(year_bucket.get(year).data or [])
    movie_ids = genre_set.intersection(year_set)
    titles = [load_title(mid) for mid in movie_ids]
    end = time.time()
    print(f"[Genre: {genre} AND Year: {year}] Found {len(titles)} movies in {end - start:.4f} seconds")

# ----------- Aggregated Queries -----------

def top_rated_by_genre(genre="Drama", top_n=5):
    start = time.time()
    top_key = top_rated_bucket.get(genre)
    movie_scores = top_key.data or []
    movie_scores.sort(key=lambda x: x[1], reverse=True)
    top = movie_scores[:top_n]
    titles = [load_title(mid) for mid, _ in top]
    end = time.time()
    print(f"Top {top_n} rated movies in Genre '{genre}':")
    for i, title in enumerate(titles, 1):
        print(f"{i}. {title}")
    print(f"Query time: {end - start:.4f} seconds\n")

def top_rated_by_genre_and_year(genre="Romance", year="2019", top_n=3):
    start = time.time()
    year_ids = set(year_bucket.get(year).data or [])
    top_key = top_rated_bucket.get(genre)
    all_scores = top_key.data or []
    common_scored = [(mid, score) for mid, score in all_scores if mid in year_ids]
    common_scored.sort(key=lambda x: x[1], reverse=True)
    top_common = common_scored[:top_n]
    titles = [load_title(mid) for mid, _ in top_common]
    end = time.time()

    print(f"Top {top_n} rated movies in Genre '{genre}' and Year '{year}':")
    if not titles:
        print("No movies found for this combination.")
    for i, title in enumerate(titles, 1):
        print(f"{i}. {title}")
    print(f"Query time: {end - start:.4f} seconds\n")

# ----------- Utility Queries -----------

def count_movies_by_actor(actor="Leonardo DiCaprio"):
    start = time.time()
    count = len(actor_bucket.get(actor).data or [])
    end = time.time()
    print(f"Number of movies with actor '{actor}': {count} (queried in {end - start:.4f} seconds)")

def count_high_rated_action_movies(min_rating=8.0):
    start = time.time()
    top_key = top_rated_bucket.get("Action")
    scores = top_key.data or []
    count = sum(1 for _, score in scores if score >= min_rating)
    end = time.time()
    print(f"Number of Action movies with rating >= {min_rating}: {count} (queried in {end - start:.4f} seconds)")

# ----------- Main -----------

if __name__ == "__main__":
    print("=== Simple Queries ===")
    query_by_genre("Action")
    query_by_actor("Tom Hanks")
    query_by_year("2015")

    print("\n=== Complex Queries ===")
    query_by_actor_and_genre("Tom Hanks", "Drama")
    query_by_genre_and_year("Action", "2015")

    print("\n=== Aggregated Queries ===")
    top_rated_by_genre("Drama", top_n=5)
    top_rated_by_genre_and_year("Romance", "2019", top_n=3)

    print("\n=== Utility Queries ===")
    count_movies_by_actor("Leonardo DiCaprio")
    count_high_rated_action_movies(8.0)
