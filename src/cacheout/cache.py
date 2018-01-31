"""The cache module provides the :class:`Cache` class which is used as the
basis for all other cache types.
"""

from collections import OrderedDict
from contextlib import suppress
from decimal import Decimal
from threading import RLock
import time


class Cache(object):
    """An in-memory cache object that supports:

    - Maximum number of cache entries
    - Global TTL default
    - Per cache entry TTL
    - TTL first/non-TTL FIFO cache eviction policy

    Cache entries are stored in an ``OrderedDict`` so that key ordering based
    on the cache type can be maintained without the need for additional
    list(s). Essentially, the key order of the ``OrderedDict`` is treated as an
    "eviction queue" with the convention that entries at the beginning of the
    queue are "newer" while the entries at the end are "older" (the exact
    meaning of "newer" and "older" will vary between different cache types).
    When cache entries need to be evicted, expired entries are removed first
    followed by the "older" entries (i.e. the ones at the end of the queue).

    Attributes:
        maxsize (int, optional): Maximum size of cache dictionary. Defaults to
            ``300``.
        ttl (int, optional): Default TTL for all cache entries. Defaults to
            ``0`` which means that entries do not expire.
        timer (callable, optional): Timer function to use to calculate TTL
            expiration. Defaults to ``time.time``.
    """
    def __init__(self, maxsize=300, ttl=0, timer=time.time):
        self._lock = RLock()

        self.configure(maxsize=maxsize, ttl=ttl, timer=timer)
        self.clear()

    def configure(self, maxsize=None, ttl=None, timer=None):
        """Configure cache settings. This method is meant to support runtime
        level configurations for global level cache objects.
        """
        if maxsize is not None:
            if not isinstance(maxsize, int):
                raise TypeError('maxsize must be an integer')

            if not maxsize >= 0:
                raise ValueError('maxsize must be greater than or equal to 0')

            self.maxsize = maxsize

        if ttl is not None:
            if not isinstance(ttl, (int, float, Decimal)):
                raise TypeError('ttl must be a number')

            if not ttl >= 0:
                raise ValueError('ttl must be greater than or equal to 0')

            self.ttl = ttl

        if timer is not None:
            if not callable(timer):
                raise TypeError('timer must be a callable')

            self.timer = timer

    def __repr__(self):
        return '{}({})'.format(self.__class__.__name__,
                               list(self.copy().items()))

    def __len__(self):
        with self._lock:
            return len(self._cache)

    def __contains__(self, key):
        with self._lock:
            return key in self._cache

    def __iter__(self):
        yield from self.copy()

    def copy(self):
        """Return a copy of the cache."""
        with self._lock:
            return self._cache.copy()

    def keys(self):
        """Return a dict view object of cache keys."""
        return self.copy().keys()

    def values(self):
        """Return a dict view object of cache values.

        Warning:
            Returned data is copied from the cache object, but any
            modifications to mutable values will also modify the cache object's
            data.
        """
        return self.copy().values()

    def items(self):
        """Return a dict view object of cache items.

        Warning:
            Returned data is copied from the cache object, but any
            modifications to mutable values will also modify the cache object's
            data.
        """
        return self.copy().items()

    def clear(self):
        """Clear all cache entries."""
        with self._lock:
            self._cache = OrderedDict()
            self._expires = {}

    def has(self, key):
        """Return whether cache key exists and hasn't expired.

        Returns:
            bool
        """
        # Use get method since it will take care of evicting expired keys.
        with suppress(KeyError):
            self.get(key)
        return key in self

    def count(self):
        """Return number of cache entries."""
        return len(self)

    def full(self):
        """Return whether the cache is full or not.

        Returns:
            bool
        """
        if self.maxsize == 0:
            return False
        return len(self) >= self.maxsize

    def get(self, key, default=None):
        """Return the cache value for `key` or `default` if it doesn't exist or
        has expired.

        Args:
            key (str): Cache key.
            default (mixed, optional): Value to return if `key` doesn't exist.
                Defaults to ``None``.

        Returns:
            mixed: The cached value.
        """
        try:
            value = self._get(key, default=default)
        except KeyError:
            value = default

        return value

    def _get(self, key, default=None):
        value = self._cache[key]

        if self.expired(key):
            self.delete(key)
            raise KeyError('Key {!r} is expired')

        return value

    def get_many(self, keys, default=None):
        """Return many cache values as a ``dict`` of key/value pairs.

        Args:
            keys (list): List of cache keys.
            default (mixed, optional): Value to return if key doesn't exist.
                Defaults to ``None``.

        Returns:
            dict
        """
        return {key: self.get(key, default=default) for key in keys}

    def add(self, key, value, ttl=None):
        """Add cache key/value if it doesn't already exist. Essentially, this
        method ignores keys that exist which leaves the original TTL in tact.

        Args:
            key (str): Cache key to add.
            value (mixed): Cache value.
            ttl (int, optional): TTL value. Defaults to ``None`` which uses
                :attr:`ttl`.
        """
        if self.has(key):
            return
        self.set(key, value, ttl=ttl)

    def add_many(self, items, ttl=None):
        """Add multiple cache keys at once.

        Args:
            items (dict): Mapping of cache key/values to set.
        """
        for key, value in items.items():
            self.add(key, value, ttl=ttl)

    def set(self, key, value, ttl=None):
        """Set cache key/value and replace any previously set cache key. If the
        cache key previous existed, setting it will move it to the end of the
        cache stack which means it would be evicted last.

        Args:
            key (str): Cache key to set.
            value (mixed): Cache value.
            ttl (int, optional): TTL value. Defaults to ``None`` which uses
                :attr:`ttl`.
        """
        if ttl is None:
            ttl = self.ttl

        if key not in self:
            self.evict()

        # Set key and move it to the end of the stack to simulate FIFO since
        # cache entries are deleted from the front first.
        with self._lock:
            self._cache[key] = value
            self._cache.move_to_end(key)

            if ttl > 0:
                self._expires[key] = self.timer() + ttl

    def set_many(self, items, ttl=None):
        """Set multiple cache keys at once.

        Args:
            items (dict): Mapping of cache key/values to set.
        """
        for key, value in items.items():
            self.set(key, value, ttl=ttl)

    def delete(self, key):
        """Delete cache key and return number of entries deleted (``1`` or
        ``0``).

        Returns:
            int: ``1`` if key was deleted, ``0`` if key didn't exist.
        """
        count = 0

        with self._lock:
            with suppress(KeyError):
                del self._cache[key]
                count = 1

            with suppress(KeyError):
                del self._expires[key]

        return count

    def delete_many(self, keys):
        """Delete multiple cache keys at once.

        Args:
            keys (list): List of cache keys.

        Returns:
            int: Number of cache keys deleted.
        """
        count = 0

        for key in keys:
            count += self.delete(key)

        return count

    def delete_expired(self):
        """Delete expired cache keys and return number of entries deleted.

        Returns:
            int: Number of entries deleted.
        """
        # Use a static expiration time for each key for better consistency as
        # opposed to a newly computed timestamp on each iteration.
        expires_on = self.timer()
        expired_keys = (key for key in self.expirations()
                        if self.expired(key, expires_on=expires_on))
        count = 0

        for key in expired_keys:
            self.delete(key)
            count += 1

        return count

    def expired(self, key, expires_on=None):
        """Return whether cache key is expired or not.

        Args:
            key (str): Cache key.
            expires_on (float, optional): Timestamp of when the key is
                considered expired. Defaults to ``None`` which uses the current
                value returned from :meth:`timer`.

        Returns:
            bool
        """
        if not expires_on:
            expires_on = self.timer()

        try:
            return self._expires[key] <= expires_on
        except KeyError:
            return key not in self

    def expirations(self):
        """Return cache expirations for TTL keys.

        Returns:
            dict
        """
        with self._lock:
            return self._expires.copy()

    def evict(self, minimum=1):
        """Perform cache eviction per the cache replacement policy:

        - First, remove **all** expired entries.
        - Then, remove non-TTL entries using using FIFO.

        Args:
            minimum (int, optional): Minimum number of cache entries to evict
                if the cache is full.

        Returns:
            int: Number of cache entries evicted.
        """
        count = 0
        number_to_delete = len(self) - self.maxsize + minimum

        if not self.full() or number_to_delete < 0:
            return count

        count += self.delete_expired()

        while count < number_to_delete:
            # Optimize method for getting the first cache entry. We need to
            # break each time since we are deleting entries (i.e. can't loop
            # through the cache while deleting it).
            for key in self._cache:
                break

            count += self.delete(key)

        return count