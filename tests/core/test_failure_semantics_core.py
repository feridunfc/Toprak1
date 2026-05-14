
from hfa.runtime.failure_semantics import FailureCategory, classify_redis_error


def test_classify_transient_errors():
    assert classify_redis_error(Exception("Timeout while reading from socket")) == FailureCategory.TRANSIENT
    assert classify_redis_error(Exception("Connection reset by peer")) == FailureCategory.TRANSIENT


def test_classify_terminal_errors():
    assert classify_redis_error(Exception("NOSCRIPT No matching script")) == FailureCategory.TERMINAL
    assert classify_redis_error(Exception("WRONGTYPE Operation against a key holding the wrong kind of value")) == FailureCategory.TERMINAL


def test_classify_unknown_errors():
    assert classify_redis_error(Exception("mysterious backend wobble")) == FailureCategory.UNKNOWN
