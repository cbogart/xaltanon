'''
Created on Mar 27, 2015

@author: bogart-MBP-isri
'''
import MySQLdb
import _mysql_exceptions
import json
import re
import os
import pdb
from collections import defaultdict


def load_matcher():
    """Prepare a dictionary of keywords to look for in execution paths"""
    with open("appinfo.XALT.json","r") as f:
        appinfo = json.load(f)
        
    matcher = { match : appinfo[app] for app in appinfo for match in appinfo[app]["match"] if match != ''}
    return matcher

def splitup(path):
    parts = re.split(r"""[_/,\.;:+=-]""", path)
    return [p.lower() for p in parts]

def just_alpha_words(path):
    parts = re.split(r"""[^a-zA-Z]*""", path)
    return [p.lower() for p in parts if len(p) >= 3]

assert just_alpha_words("a/b/word234x") == ["word"], "just_alpha_words returned " + str(just_alpha_words("a/b/word234x"))

def guess_unclassified_app(exec_path):
    try:
        return re.search(r"([a-zA-Z]{3,})", exec_path.split("/")[-1]).group(0)
    except:
        return ""
    
assert guess_unclassified_app("a/b/3cdef4.exe") == "cdef", "guess_unclassified_app" + guess_unclassified_app("a/b/3cdef4.exe")
import pdb
def classify(exec_path, matcher):
    candidates = []
    possibles = set([p.lower() for p in exec_path.split("/")]).intersection(set(matcher.keys()))
    if len(possibles) == 0:
        possibles = set(splitup(exec_path)).intersection(set(matcher.keys()))
    if len(possibles) == 0:
        possibles = set(just_alpha_words(exec_path)).intersection(set(matcher.keys()))
    for package in possibles:
        try:
            versionmatch = re.search(matcher[package]["versionPattern"], exec_path).group(0)
        except AttributeError:
            versionmatch = ""
        candidates = candidates + [ (matcher[package]["title"], versionmatch)]
    candidates.sort(key = lambda (p,v): len(p)*len(v))

    if "python" in exec_path and (len(candidates) == 0 or candidates[0][0] != "Python"):
        print exec_path, "\t", candidates
        #pdb.set_trace()

    return candidates[0] if len(candidates) > 0 else ("","")
            
def assertEquals(a, b):
    assert a == b, str(a) + " should have been " + str(b)
    
matcher = load_matcher()

assertEquals(classify("/opt/apps/intel13/mvapich2_1_9/amber/12.0/bin/pmemd.cuda_SPFP", matcher), 
             ("AmberTools", "12.0"))
assertEquals(classify("/scratch/4396/U4396/Gromacs/install/bin/mdrun_mpi", matcher),
             ("GROMACS", ""))
assertEquals(classify("/work/6376/U6376/NAMD-build-2014-May-12-22761/NAMD_2.9_Linux-x86_64-MVAPICH-Intel-Stampede/namd2", matcher),
             ("NAMD", "2.9"))
if __name__ == '__main__':
    db = MySQLdb.connect(host="localhost", # your host, usually localhost
                     user="root", # your username
                      passwd="", # your password
                      db="xalt") # name of the data base
    cur = db.cursor()
    
    cur.execute("select exec_path, user from xalt_run_anon;")
    bad_cloud = defaultdict(int)
    found = 0
    not_found = 0
    app_counts = defaultdict(int)
    app_versions = defaultdict(set)
    unknown_app_users = defaultdict(set)
    unknown_app_count = defaultdict(int)
    try:
        for row in cur.fetchall():
            if ((found + not_found) % 1000 == 0):
                print "Found", found, "out of", (found + not_found), "examined so far"
            exec_path = row[0]
            username = row[1]
            (package, version) =  classify(exec_path, matcher)
            if len(package) > 0:
                #print package + "/" + version, exec_path
                found+=1
                app_counts[package] += 1
                if len(version) > 0:
                    app_versions[package].add(version)
            else:
                not_found += 1
                package = guess_unclassified_app(exec_path)
                unknown_app_count[package] += 1
                unknown_app_users[package].add(username)
                for part in splitup(exec_path):
                    bad_cloud[part.lower()]+=1
    finally:
        print "Known apps----------------------"
        for app in sorted(app_counts.keys()):
            print app, app_counts[app]
        print "Known app versions----------------------"
        for app in sorted(app_counts.keys()):
            print app, app_counts[app], "versions:", ",".join(app_versions[app])
        print "Unknown apps---------------------"
        for app in sorted(unknown_app_count.keys()):
            if len(unknown_app_users[app]) >= 5:
                print app, unknown_app_count[app]
        print "Found percentage", (found*100/(found+not_found))
        #print "Tag cloud dump:"
        #print json.dumps(sorted([(bad_cloud[k], k) for k in bad_cloud], key=lambda (p,v): -p)[0:400])