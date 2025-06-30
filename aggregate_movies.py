import riak
import ast
import json


riak_db = riak.RiakClient(pb_port=8087, protocol='pbc')  # Protocol buffers

movies_bucket = riak_db.bucket('movies')
genre_bucket = riak_db.bucket('genre')
actor_bucket = riak_db.bucket('actor')
year_bucket = riak_db.bucket('year')
top_rated_bucket = riak_db.bucket('top_rated')

def clean_bucket(bucket):
    print(f"Cleaning bucket: {bucket.name}")
    for key in bucket.get_keys():
        bucket.delete(key)

def aggregate_data():
    print("Cleaning up old aggregation keys...")
    clean_bucket(genre_bucket)
    clean_bucket(actor_bucket)
    clean_bucket(year_bucket)
    clean_bucket(top_rated_bucket)
    print("Cleanup complete.")

    LIMIT = 5000
    processed = 0

    print("Starting data aggregation...")
    for key in movies_bucket.get_keys():
        if processed >= LIMIT:
            break

        movie_obj = movies_bucket.get(key)
        if not movie_obj.exists:
            continue

        try:
            movie = json.loads(movie_obj.data)
            movie_id = str(movie.get("id"))
            vote_avg = float(movie.get("vote_average", 0))
            year_raw = movie.get("release_year")
            year = None

            if isinstance(year_raw, (int, float)):
                year = str(int(year_raw))
            elif isinstance(year_raw, str) and year_raw.strip().isdigit():
                year = str(int(year_raw.strip()))

            main_cast = movie.get("Star1")
            genres_raw = movie.get("genres_list", "[]")
            try:
                genres = ast.literal_eval(genres_raw) if isinstance(genres_raw, str) else genres_raw
                genres = [str(g).strip() for g in genres if g]
            except:
                genres = []

            movie_key = f"movie:{movie_id}"

            # ACTOR
            if main_cast and isinstance(main_cast, str):
                actor_clean = main_cast.strip()
                if actor_clean:
                    actor_key = actor_bucket.get(actor_clean)
                    existing = actor_key.data or []
                    if movie_key not in existing:
                        existing.append(movie_key)
                    actor_key.data = existing
                    actor_key.store()

            # GENRE
            for genre in genres:
                if genre:
                    genre_key = genre_bucket.get(genre)
                    genre_movies = genre_key.data or []
                    if movie_key not in genre_movies:
                        genre_movies.append(movie_key)
                    genre_key.data = genre_movies
                    genre_key.store()

                    # TOP RATED
                    top_key = top_rated_bucket.get(genre)
                    top_list = top_key.data or []
                    top_list.append((movie_key, vote_avg))
                    # Sort descending by vote average and trim
                    top_list = sorted(top_list, key=lambda x: x[1], reverse=True)[:100]
                    top_key.data = top_list
                    top_key.store()

            # YEAR
            if year:
                year_key = year_bucket.get(year)
                year_movies = year_key.data or []
                if movie_key not in year_movies:
                    year_movies.append(movie_key)
                year_key.data = year_movies
                year_key.store()

            processed += 1
            if processed % 100 == 0:
                print(f"Processed {processed} movies...")

        except Exception as e:
            print(f"Error processing {key}: {e}")

    print(f"✅ Aggregation completed for {processed} movies.")

if __name__ == "__main__":
    aggregate_data()
