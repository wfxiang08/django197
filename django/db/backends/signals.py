# -*- coding:utf-8 -*-
from django.dispatch import Signal

# 目前似乎只在: spatialite中使用
connection_created = Signal(providing_args=["connection"])
