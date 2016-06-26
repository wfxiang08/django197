# -*- coding:utf-8 -*-
from django.core import signals
from django.db.utils import (
    DEFAULT_DB_ALIAS, DJANGO_VERSION_PICKLE_KEY, ConnectionHandler,
    ConnectionRouter, DatabaseError, DataError, Error, IntegrityError,
    InterfaceError, InternalError, NotSupportedError, OperationalError,
    ProgrammingError,
)

__all__ = [
    'backend', 'connection', 'connections', 'router', 'DatabaseError',
    'IntegrityError', 'InternalError', 'ProgrammingError', 'DataError',
    'NotSupportedError', 'Error', 'InterfaceError', 'OperationalError',
    'DEFAULT_DB_ALIAS', 'DJANGO_VERSION_PICKLE_KEY'
]

# 管理各种alias的请求
connections = ConnectionHandler()

# 路由
router = ConnectionRouter()


# `connection`, `DatabaseError` and `IntegrityError` are convenient aliases
# for backend bits.

# DatabaseWrapper.__init__() takes a dictionary, not a settings module, so we
# manually create the dictionary from the settings, passing only the settings
# that the database backends care about.
# We load all these up for backwards compatibility, you should use
# connections['default'] instead.
class DefaultConnectionProxy(object):
    """
    Proxy for accessing the default DatabaseWrapper object's attributes. If you
    need to access the DatabaseWrapper object itself, use
    connections[DEFAULT_DB_ALIAS] instead.
    """

    def __getattr__(self, item):
        return getattr(connections[DEFAULT_DB_ALIAS], item)

    def __setattr__(self, name, value):
        return setattr(connections[DEFAULT_DB_ALIAS], name, value)

    def __delattr__(self, name):
        return delattr(connections[DEFAULT_DB_ALIAS], name)

    def __eq__(self, other):
        return connections[DEFAULT_DB_ALIAS] == other

    def __ne__(self, other):
        return connections[DEFAULT_DB_ALIAS] != other


"""
数据库用法1：
    from django.db import connection
数据库用法2:
    from django.db import connections
    connections[alias]
"""
# 默认的数据库连接
connection = DefaultConnectionProxy()


# Register an event to reset saved queries when a Django request is started.
def reset_queries(**kwargs):
    # 在每一个connection上会有 queries_log 的记录
    for conn in connections.all():
        conn.queries_log.clear()


#
# 请求开始时 reset_queries
# djang debug toolbar等可以用来分析mysql query的执行情况
#
signals.request_started.connect(reset_queries)


# Register an event to reset transaction state and close connections past
# their lifetime.
def close_old_connections(**kwargs):
    for conn in connections.all():
        conn.close_if_unusable_or_obsolete()


# "请求开始"或"请求结束"时关闭所有的 old connections
signals.request_started.connect(close_old_connections)
signals.request_finished.connect(close_old_connections)
