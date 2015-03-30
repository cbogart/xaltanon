import subprocess
from collections import defaultdict
import json

def parse_spider_avail(spider_output):
    for line in spider_output.split("\n"):
        if "Where:" in line:
            break
        if "-----" in line:
            continue
        line = line.replace("(m)","").replace("(D)","").strip()
        if line == "":
            continue
        for part in line.split():
            yield part.strip()

def parse_spider_whatis(spider_output):
    for line in spider_output.split("\n"):
        parts = line.split(":")
        if len(parts) > 2:
            yield (parts[0].strip(), parts[1].strip(), (":".join(parts[2:])).strip())
    
def module_avail():
    return (parse_spider_avail(subprocess.check_output("module avail", shell=True, stderr=subprocess.STDOUT)))

def module_detail(package_version):
    return parse_spider_whatis(subprocess.check_output("module whatis " + package_version, shell=True, stderr=subprocess.STDOUT))

if __name__ == '__main__':
    packages = list(module_avail())
    module_info = defaultdict(lambda: defaultdict(dict))
    for pack in packages:
        print pack
        inf = dict()
        try:
            for details in module_detail(pack):
                inf[details[1]] = details[2]
            name = inf.get("Name", pack.split("/")[0])
            version = inf.get("Version", pack.split("/")[-1])
            inf["moduleid"] = pack
            module_info[name][version] = inf
        except Exception, e:
            print str(e)
            module_info["errors"][pack] = str(e)
        
    with open("moduleInfo.json","w") as f:
        f.write(json.dumps(module_info, indent=4))
