# coding: utf-8
import threading
import logging

import requests


class Pool(object):
    """
    Gives you a free HTTP session to Elliptics from pool.
    """
    __pool = None

    def __init__(self, max_sessions=5):
        self.__free_sessions_counter = threading.BoundedSemaphore(max_sessions)
        self.max_sessions = max_sessions
        self.__pool = set([
            new_session(keep_alive=True) for i in xrange(max_sessions)
        ])
        self.logger = logging.getLogger('django_elliptics.http.pool')
        self.lock = threading.RLock()

    def free_session(self):
        self.__free_sessions_counter.acquire()
        self.lock.acquire()
        try:
            session = self.__pool.pop()
            self.logger.info(
                'Got http session from pool ("%s")', repr(session)
            )
            return session
        finally:
            self.lock.release()

    def release_session(self, session):
        """
        @type session: requests.Session
        """
        try:
            self.__free_sessions_counter.release()
            self.lock.acquire()
            self.__pool.add(session)
            self.logger.info(
                'Put http session back into pool ("%s")', repr(session)
            )
            self.lock.release()
        except ValueError:
            # released too many times, we do not have so many sessions
            # probably a bug
            self.logger.error('We do not have as much sessions, as we have released')



def new_session(keep_alive):
    """

    @type keep_alive: bool
    """
    session = requests.session()
    session.config['keep_alive'] = keep_alive
    return session
