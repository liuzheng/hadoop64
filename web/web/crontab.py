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
from time import clock

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
    print version
    if os.path.isfile(PROJECT_ROOT + '/static/' + version.replace('-src', '')):
        return True
    url = mirrors(version) + '/current/'
    print(url)
    DownloadLink = url + version
    hadoop64 = '/tmp/' + version[:-7] + '/hadoop-dist/target/' + version.replace('-src', '')
    print DownloadLink
    print PROJECT_ROOT + '/static/' + version
    if not os.path.isdir(PROJECT_ROOT + '/static'):
        os.makedirs(PROJECT_ROOT + '/static')
    if os.path.isfile(PROJECT_ROOT + '/static/' + version):
        html = urllib2.urlopen(DownloadLink + '.mds').read()
        md5head = re.search(r'MD5 =(.*)', html).group(1).replace(' ', '').lower()  # only need the head, that's enough
        print md5head
        with open(PROJECT_ROOT + '/static/' + version, 'rb') as f:
            md5obj = hashlib.md5()
            md5obj.update(f.read())
            hash = md5obj.hexdigest()
            print hash
        if md5head == hash[:-2]:
            print 'the file is already exist!!'
        else:
            os.remove(PROJECT_ROOT + '/static/' + version)
            urllib.urlretrieve(DownloadLink, PROJECT_ROOT + '/static/' + version)
    else:
        urllib.urlretrieve(DownloadLink, PROJECT_ROOT + '/static/' + version)
    if os.path.isfile(PROJECT_ROOT + '/static/' + version):
        print('now extract the tar file')
        Extract(version)
        Make64Hadoop(version, hadoop64)
    else:
        # download again
        urllib.urlretrieve(DownloadLink, PROJECT_ROOT + '/static/' + version)
        print('now extract the tar file')
        Extract(version)
        Make64Hadoop(version, hadoop64)
    if not os.path.isfile(hadoop64):
        print('Error occurred ')
        return False
    Upload2BaiduPan()
    return True


def Upload2BaiduPan():
    os.chdir(PROJECT_ROOT)
    os.system('web/bypy.py upload static/ hadoopx86_64/2.x/')


def Extract(version):
    os.chdir(PROJECT_ROOT + '/static/')
    tar = tarfile.open(version)
    names = tar.getnames()
    for name in names:
        tar.extract(name, path="/tmp")
    tar.close()


def Make64Hadoop(version, hadoop64):
    # Now mvn package
    os.chdir('/tmp/' + version[:-7])
    if os.path.isfile(hadoop64):
        os.system('cp ' + hadoop64 + ' ' + PROJECT_ROOT + '/static/')
    else:
        for i in xrange(5):
            os.system('mvn package -DskipTests -Pdist,native -Dtar')
            # mvn package -DskipTests -Pdist,native -Dtar
            #     print hadoop64
            if os.path.isfile(hadoop64):
                os.system('cp ' + hadoop64 + ' ' + PROJECT_ROOT + '/static/' + version.replace('-src', '_x86_64'))
                break
                # print('cp '+hadoop64+' '+PROJECT_ROOT+'/static/')


def speed(url, version):
    start = clock()
    try:
        req = urllib2.Request(url + version)
        req.headers['Range'] = 'bytes=%s-%s' % (0, 500)
        response = urllib2.urlopen(req, timeout=1)
        end = clock()
    except:
        return 99999
    return end - start


def mirrors(version):
    mirror_list = [
        'http://mirrors.gigenet.com/apache/hadoop/common/',
        'http://apache.cs.utah.edu/hadoop/common/',
        'http://mirrors.koehn.com/apache/hadoop/common/',
        'http://mirror.reverse.net/pub/apache/hadoop/common/',
        'http://mirrors.advancedhosters.com/apache/hadoop/common/',
        'http://mirror.symnds.com/software/Apache/hadoop/common/',
        'http://www.carfab.com/apachesoftware/hadoop/common/',
        'http://apache.spinellicreations.com/hadoop/common/',
        'http://www.webhostingjams.com/mirror/apache/hadoop/common/',
        'http://mirror.nexcess.net/apache/hadoop/common/',
        'http://mirror.sdunix.com/apache/hadoop/common/',
        'http://download.nextag.com/apache/hadoop/common/',
        'http://mirror.olnevhost.net/pub/apache/hadoop/common/',
        'http://mirrors.ibiblio.org/apache/hadoop/common/',
        'http://apache.mesi.com.ar/hadoop/common/',
        'http://apache.tradebit.com/pub/hadoop/common/',
        'http://mirror.tcpdiag.net/apache/hadoop/common/',
        'http://apache.mirrors.lucidnetworks.net/hadoop/common/',
        'http://apache.mirrors.pair.com/hadoop/common/',
        'http://apache.mirrors.hoobly.com/hadoop/common/',
        'http://www.interior-dsgn.com/apache/hadoop/common/',
        'http://www.eng.lsu.edu/mirrors/apache/hadoop/common/',
        'http://apache.claz.org/hadoop/common/',
        'http://apache.petsads.us/hadoop/common/',
        'http://mirror.metrocast.net/apache/hadoop/common/',
        'http://apache.mirrors.tds.net/hadoop/common/',
        'http://apache.osuosl.org/hadoop/common/',
        'http://psg.mtu.edu/pub/apache/hadoop/common/',
        'http://mirrors.sonic.net/apache/hadoop/common/',
        'http://www.trieuvan.com/apache/hadoop/common/',
        'http://mirror.cogentco.com/pub/apache/hadoop/common/',
        'http://apache.arvixe.com/hadoop/common/',
        'http://supergsego.com/apache/hadoop/common/',
        'http://www.motorlogy.com/apache/hadoop/common/',
        'http://www.gtlib.gatech.edu/pub/apache/hadoop/common/',
        'http://mirror.cc.columbia.edu/pub/software/apache/hadoop/common/'
    ]
    sp = []
    for i in mirror_list:
        # print i
        sp.append(speed(i + 'current/', version))
        # print sp[-1]
    return mirror_list[sp.index(min(sp))]


if __name__ == '__main__':
    CheckNewVersion()

# url = 'http://apache.osuosl.org/hadoop/common/current/hadoop-2.6.0-src.tar.gz'
# req = urllib2.Request(url)
# req.headers['Range'] = 'bytes=%s-%s' % (0, 500)
# response = urllib2.urlopen(req)
# print(response.read())

