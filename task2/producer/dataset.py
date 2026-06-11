from faker import Faker
from typing import Generator

SUBREDDITS = [
    "python", "datascience", "machinelearning", "programming", "technology",
    "worldnews", "science", "askreddit", "gaming", "movies",
]


def _make_comment(index: int, fake: Faker, producer_id: int) -> dict:
    return {
        "id": f"t1_{fake.lexify('???????')}",
        "subreddit": fake.random_element(SUBREDDITS),
        "author": fake.user_name(),
        "body": fake.paragraph(nb_sentences=fake.random_int(min=1, max=5)),
        "created_utc": float(fake.unix_time(start_datetime="-2y")),
        "score": fake.random_int(min=-10, max=5000),
        "message_index": index,
        "producer_id": f"producer-{producer_id}",
    }


def generate_dataset(
    num_messages: int,
    num_producers: int,
    producer_id: int,
    seed: int,
) -> Generator[dict, None, None]:
    """Yield comments assigned to this producer (index % num_producers == producer_id)."""
    fake = Faker()
    Faker.seed(seed)
    for i in range(num_messages):
        # Re-advance the Faker state consistently regardless of which producer runs this
        comment = _make_comment(i, fake, producer_id)
        if i % num_producers == producer_id:
            yield comment
