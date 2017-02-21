#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Binux<roy@binux.me>
#         http://binux.me
# Created on 2014-11-22 20:30:44

import os
import time
import sqlite3
import threading


class SQLiteMixin(object):

    @property
    def dbcur(self):
        # 进程pid，线程ident（进程，线程的标识符）
        pid = (os.getpid(), threading.current_thread().ident)
        # 如果已经有self.conn并且pid相等
        if not (self.conn and pid == self.last_pid):
            # self.last_pid为当前的pid
            self.last_pid = pid
            # 创建连接
            self.conn = sqlite3.connect(self.path, isolation_level=None)
        return self.conn.cursor()


class SplitTableMixin(object):
    # 更新project的时间间隔
    UPDATE_PROJECTS_TIME = 10 * 60

    def _tablename(self, project):
        """返回tablename"""
        if self.__tablename__:
            return '%s_%s' % (self.__tablename__, project)
        else:
            return project

    @property
    def projects(self):
        """返回project(若时间差大于时间间隔，更新project)"""
        if time.time() - getattr(self, '_last_update_projects', 0) \
                > self.UPDATE_PROJECTS_TIME:
            self._list_project()
        return self._projects

    @projects.setter
    def projects(self, value):
        self._projects = value

    def _list_project(self):
        """从数据库中读取project到self.project中"""
        self._last_update_projects = time.time()
        self.projects = set()
        if self.__tablename__:
            prefix = '%s_' % self.__tablename__
        else:
            prefix = ''
        for project, in self._select('sqlite_master', what='name',
                                     where='type = "table"'):
            # 从DB中读取所有table
            if project.startswith(prefix):
                # 如果是以"projectdb"/"resultdb"/"taskdb"为前缀
                project = project[len(prefix):]
                # 将peoject名添加入self.projects
                self.projects.add(project)

    def drop(self, project):
        """从数据库中删除project"""
        if project not in self.projects:
            self._list_project()
        if project not in self.projects:
            return
        tablename = self._tablename(project)
        self._execute("DROP TABLE %s" % self.escape(tablename))
        self._list_project()
