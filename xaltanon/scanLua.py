import os
import re
import json
from collections import defaultdict

def scan_modulefiles(modulefiles_path):
    modules = defaultdict(lambda: defaultdict(dict))
    for path, _, files in os.walk(modulefiles_path):
      try:
        if "modulefiles" in path:
            for lua in files:
                if lua.endswith(".lua"):
                    contents = open(path + "/" + lua).readlines()
                    version = lua.replace(".lua","")
                    modulename = path.split("/")[-1]
                    for line in contents:
                        meta = re.match(r'''.*whatis.*"(.*?): (.*)".*''', line)
                        if meta:
                            modules[modulename][version][meta.group(1)] = meta.group(2)
      except Exception, e:
        print "Skipping",path,"because",str(e)
    return modules

def combine_with_appinfo(mods):

    app_info = { a["title"]: a for a in json.load(open("metadata/appinfo.TACC.json"))}
    
    print "AmberTools check1:", app_info["AmberTools"]
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

    print "AmberTools check2:", app_info["AmberTools"]
    
    with open("appinfo.XALT.json", "w") as f:
        f.write(json.dumps(app_info, indent=4))  
        
if __name__ == '__main__':
    mods = scan_modulefiles("/opt/apps")
    with open("moduleInfoFromLua.json", "w") as f:
        f.write(json.dumps(mods, indent=4))

