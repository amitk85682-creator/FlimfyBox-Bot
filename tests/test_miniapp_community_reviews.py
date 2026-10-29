"""First-party FlimfyBox community movie reviews."""

import pytest

from .conftest import FakeConnection, FakeCursor, signed_init_data


class ReviewCursor(FakeCursor):
    def fetchone(self):
        query = self.last_query.lower()
        if 'select is_unreleased from movies' in query:
            return self.connection.movie_row
        if 'select coalesce(avg(rating), 0), count(*)' in query:
            return self.connection.rating_row
        if 'select rating from movie_ratings' in query:
            return (self.connection.user_rating,) if self.connection.user_rating is not None else None
        if 'select count(*) from movie_reviews' in query:
            return (self.connection.review_count,)
        if 'select review from movie_reviews' in query:
            return (self.connection.user_review,) if self.connection.user_review else None
        return super().fetchone()

    def fetchall(self):
        return self.connection.review_rows


class ReviewConnection(FakeConnection):
    def __init__(self, *, movie_row=(False,), review_rows=None, review_count=0,
                 user_review=None, rating_row=(0, 0), user_rating=None):
        super().__init__()
        self.movie_row = movie_row
        self.review_rows = review_rows or []
        self.review_count = review_count
        self.user_review = user_review
        self.rating_row = rating_row
        self.user_rating = user_rating

    def cursor(self):
        return ReviewCursor(self)


def signed_headers():
    return {'X-Telegram-Init-Data': signed_init_data()}


def test_public_flimfy_rating_summary_supplies_the_third_verdict_source(webapp_factory):
    connection = ReviewConnection(rating_row=(4.3, 12))
    app, _, _ = webapp_factory(connection=connection)

    response = app.test_client().get('/api/movie/12/rating')

    assert response.status_code == 200
    assert response.json['average'] == 4.3
    assert response.json['count'] == 12
    assert response.json['user_rating'] is None
    assert response.json['can_rate'] is False


def test_reviews_are_first_party_and_readable_without_sign_in(webapp_factory):
    connection = ReviewConnection(
        review_rows=[('A warm, funny story with a great ending.', 'Mahi', '2026-10-28T10:00:00Z', 5)],
        review_count=4,
    )
    app, _, _ = webapp_factory(connection=connection)

    response = app.test_client().get('/api/movie/12/reviews')

    assert response.status_code == 200
    assert response.json == {
        'status': 'success',
        'reviews': [{
            'content': 'A warm, funny story with a great ending.',
            'author': 'Mahi',
            'created_at': '2026-10-28T10:00:00Z',
            'rating': 5,
        }],
        'review_count': 4,
        'user_review': None,
        'can_review': False,
        'is_upcoming': False,
    }
    assert not any('/reviews' in query for query, _params in connection.queries if 'themoviedb' in query)


def test_signed_member_gets_own_review_and_can_edit_it(webapp_factory):
    connection = ReviewConnection(user_review='My earlier review is long enough.')
    app, _, _ = webapp_factory(connection=connection)

    response = app.test_client().get('/api/movie/12/reviews', headers=signed_headers())

    assert response.status_code == 200
    assert response.json['user_review'] == 'My earlier review is long enough.'
    assert response.json['can_review'] is True


def test_review_submission_requires_telegram_identity(webapp_factory):
    app, _, _ = webapp_factory(connection=ReviewConnection())

    response = app.test_client().post('/api/movie/12/reviews', json={'review': 'A long enough review of this movie.'})

    assert response.status_code == 401


@pytest.mark.parametrize('review', ['', 'Too short.', 'x' * 801, 12, None])
def test_review_submission_validates_plain_text_length(webapp_factory, review):
    app, _, connection = webapp_factory(connection=ReviewConnection())

    response = app.test_client().post(
        '/api/movie/12/reviews', json={'review': review}, headers=signed_headers()
    )

    assert response.status_code == 400
    assert not any('INSERT INTO movie_reviews' in query for query, _params in connection.queries)


def test_review_submission_upserts_one_member_review(webapp_factory):
    app, _, connection = webapp_factory(connection=ReviewConnection())
    review = 'A warm, funny story with a great ending.'

    response = app.test_client().post(
        '/api/movie/12/reviews', json={'review': f'  {review}  '}, headers=signed_headers()
    )

    assert response.status_code == 200
    assert response.json['status'] == 'success'
    assert response.json['review'] == review
    insert_query, params = next(
        (query, params) for query, params in connection.queries
        if 'INSERT INTO movie_reviews' in query
    )
    assert 'ON CONFLICT (movie_id, user_id) DO UPDATE' in insert_query
    assert params == (12, 101, review)
    assert connection.commits == 1


def test_upcoming_title_cannot_receive_member_reviews(webapp_factory):
    app, _, connection = webapp_factory(connection=ReviewConnection(movie_row=(True,)))

    response = app.test_client().post(
        '/api/movie/12/reviews',
        json={'review': 'This is a sufficiently long review.'},
        headers=signed_headers(),
    )

    assert response.status_code == 409
    assert not any('INSERT INTO movie_reviews' in query for query, _params in connection.queries)
