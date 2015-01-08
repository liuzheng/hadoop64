#!/usr/bin/env python
# coding: utf-8
# Copyright (c) 2015
# Gmail:liuzheng712
#
__author__ = 'liuzheng'
import os
import sys
import json
import time
import urllib
import urllib2
import re
import hashlib
import tarfile

parentdir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parentdir)

try:
    import cPickle as pkl
except:
    import pickle as pkl
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))


def CheckNewVersion():
    url = 'http://apache.osuosl.org/hadoop/common/current/'
    html = urllib2.urlopen(url).read()
    version = re.search(r'hadoop-.*?src\.tar\.gz', html).group()
    if os.path.isfile(PROJECT_ROOT + '/static/' + version.replace('-src', '')):
        return
    DownloadLink = url + version
    # print DownloadLink
    if os.path.isfile(version):
        html = urllib2.urlopen(DownloadLink + '.mds').read()
        md5head = re.search(r'MD5 =(.*)', html).group(1).replace(' ', '').lower()  # only need the head, that's enough
        # print md5head
        with open(version, 'rb') as f:
            md5obj = hashlib.md5()
            md5obj.update(f.read())
            hash = md5obj.hexdigest()
            # print hash
        if md5head == hash[:-2]:
            print 'the file is already exist!!'
        else:
            os.remove(version)
            urllib.urlretrieve(DownloadLink, version)
    else:
        urllib.urlretrieve(DownloadLink, version)
    if os.path.isfile('../static/' + version):
        print('file is make done')
    else:
        print('now extract the tar file')
        tar = tarfile.open(version)
        names = tar.getnames()
        for name in names:
            tar.extract(name, path="/tmp")
        tar.close()
        os.chdir('/tmp/' + version[:-7])
        os.system('mvn package -DskipTests -Pdist,native -Dtar')
        # mvn package -DskipTests -Pdist,native -Dtar
        hadoop64 = 'hadoop-dist/target/' + version.replace('-src', '')
        print hadoop64
        if os.path.isfile(hadoop64):
            os.system('cp ' + hadoop64 + ' ' + PROJECT_ROOT + '/static/')
            # print('cp '+hadoop64+' '+PROJECT_ROOT+'/static/')


def make64hadoop():
    return 0


if __name__ == '__main__':
    CheckNewVersion()