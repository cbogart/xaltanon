'''
Created on Mar 27, 2015

@author: bogart-MBP-isri
'''
import os
import re
import json
from collections import defaultdict

def scan_modulefiles(modulefiles_path):
    """Recurse through the /opt/apps module directories and get module information
    
    Necessary because "module spider" does not return the "whatis" metadata
    from each package (such as URL, description).
    """
    modules = defaultdict(lambda: defaultdict(dict))
    for path, _, files in os.walk(modulefiles_path):
        for lua in files:
            if lua.endswith(".lua") and "modulefiles" in path:
                contents = open(path + "/" + lua).readlines()
                version = lua.replace(".lua","")
                modulename = path.split("/")[-1]
                for line in contents:
                    meta = re.match(r'''.*whatis.*"(.*?): (.*)".*''', line)
                    if meta:
                        modules[modulename][version][meta.group(1)] = meta.group(2)
    return modules

def generalize_versions(versions):
    """Turn a list of version numbers into a regex that recognizes them
    
    A hard problem in general, but make lots of assumptions."""
    if versions == []:
        return ""
    regexes = set()
    for ver in versions:
        # 8-digit date as version
        ver = re.sub(r"(\d{8})", "\d{8}", ver)
        # Number followed by letter
        ver = re.sub(r"(\d+[a-zA-Z]\b)", "\\d+[a-zA-Z]", ver)
        # Other numbers (but not the {8} we introduced ourselves):
        ver = re.sub(r"(\d+)(?!})", "\\d+", ver)
        # Other strings (long enough not to be the zA we introduced ourselves)
        ver = re.sub(r"([a-zA-Z]{3,})", "[a-zA-Z]+", ver)

        # dots
        ver = re.sub(r"(\.)", "\\.", ver)
        
        regexes.add(ver)
    return "(" + ")|(".join(sorted(regexes, key= lambda reg: -len(reg))) + ")"

def test_generalize(versions, regex, tester, should_match):
    generated_regex = generalize_versions(versions)
    assert generated_regex == regex, str(versions) + " yields " + generated_regex + " not " + regex
    matches = re.search(generated_regex, tester)
    match = matches.group(0) or ""
    assert match == should_match, "versions " + str(versions) + " search on: " + tester + " using: " + generated_regex + " are: " + match + " not: " + should_match

test_generalize(["3.2", "8.7"], "(\d+\.\d+)", "This is version7.8", "7.8")
test_generalize(["3.2.1", "8.7"], "(\d+\.\d+\.\d+)|(\d+\.\d+)", "This is version7.8.borgia", "7.8")
test_generalize(["3.2.1", "8.7"], "(\d+\.\d+\.\d+)|(\d+\.\d+)", "This is version7.8.1234borgia", "7.8.1234")
test_generalize(["3.2.1b", "8.7.3"], "(\d+\.\d+\.\d+[a-zA-Z])|(\d+\.\d+\.\d+)", "This is version7.8.1234a", "7.8.1234a")
test_generalize(["3.2", "8.7"], "(\d+\.\d+)", "This is not version 3.2 but version 7.8", "3.2")
test_generalize(["2013wk43"], "(\d+wk\d+)", "version/2015wk15", "2015wk15")
test_generalize([
            "3.5-complexdebug", 
            "3.4-cxxdebug", 
            "3.5-complex", 
            "3.4-cxxcomplex", 
            "3.4-cxx", 
            "3.4-cxxcomplexdebug", 
            "3.5-debug", 
            "3.5-cxxcomplexdebug", 
            "3.4", 
            "3.5-cxx", 
            "3.5", 
            "3.5-cxxcomplex", 
            "3.4-debug", 
            "3.4-complexdebug", 
            "3.4-complex", 
            "3.5-cxxdebug"
        ], "(\d+\.\d+-[a-zA-Z]+)|(\d+\.\d+)", "its/version/13.5-cxx/I/think", "13.5-cxx")

def combine_with_appinfo(mods, old_app_file, new_app_file):
    """Combine module metadata with preexisting appinfo.json file"""
    app_info = { a["title"]: a for a in json.load(open(old_app_file))}
    
    for t in app_info:
        app_info[t]["provenance"] = "appinfo"
        if t.lower() not in app_info[t]["match"]:
            app_info[t]["match"] += [t.lower()]
    app_info_lookup = { m : title for title in app_info for m in app_info[title]["match"]}
    for mod in mods:
        print mod
        if mod == "errors": 
            continue
        modl = mod.lower()
        modv = mods[mod][mods[mod].keys()[0]]
        views = list(set([v.strip().lower() for v in (modv.get("Category","").split(",") + modv.get("Keywords","").split(",")) if v.strip() != ""]))
        if modl not in app_info_lookup:
            app_info[modl] = dict()
            app_info[modl]["title"] = mod
            app_info[modl]["match"] = [modl]
            app_info[modl]["description"] = modv.get("Description","")
            app_info[modl]["website"] = modv.get("URL","")
            app_info[modl]["versions"] = mods[mod].keys()
            app_info[modl]["views"] = views          
            app_info[modl]["provenance"] = "stampede module"  
        else: # mod in app_info_lookup:
            t = app_info_lookup[modl]
            app_info[t]["versions"] = mods[mod].keys()
            app_info[t]["views"] = views            
            app_info[t]["short_description"] = modv.get("Description","") + "\n" + app_info[t]["short_description"]
            if "website" not in app_info[t] or app_info[t]["website"] == "":
                app_info[t]["website"] = modv.get("URL","")
            else:
                if "URL" in modv: app_info[t]["website2"] = modv["URL"]
            app_info[t]["provenance"] = "appinfo + stampede module"

    for app in app_info:
        app_info[app]["versionPattern"] = generalize_versions(app_info[app].get("versions",[]))
    
    with open(new_app_file, "w") as f:
        f.write(json.dumps(app_info, indent=4))  
        
if __name__ == '__main__':
    with open("moduleInfoFromLua.json", "r") as f:
        mods = json.loads(f.read())
    combine_with_appinfo(mods, "metadata/appinfo.TACC.json", "appinfo.XALT.json")

                                         