'''
Created on Apr 7, 2015

@author: bogart-MBP-isri
'''
import xaltanon.gen_aliases
# easy_install mysql-connector-python
import mysql.connector
import json

def dict_slice(d,s):
    return { k:d[k] for k in d if k in s }

def entryToEpoch(d,k):
    try:
        d[k] = time.mktime(d[k].timetuple())
    except:
        pass

def link_info(db):
    cur = db.cursor(dictionary=True) 
    print "Loading all link information"
    query_params = {
        "xalt_run": "xalt_run",
        "xalt_link":"xalt_link",
        "xalt_object": "xalt_object",
        "join_run_object": "join_run_object",
        "join_link_object": "join_link_object",
        
        }
    
    link_query = """
        select * from {xalt_link} 
        left join {join_link_object} 
           on {xalt_link}.link_id={join_link_object}.link_id 
        left join {xalt_object} as obj_link
           on {join_link_object}.obj_id=obj_link.obj_id """.format(**query_params)
    print link_query
    cur.execute(link_query)
    link_info = {}
    counter = 0
    for row in cur.fetchall():
        if counter%1000 == 0:
            print counter
        counter += 1
        if "uuid" not in link_info:
            link_info["uuid"] = []
        link_info["uuid"].append(dict_slice(row,
                    ["object_path", "module_name", "timestamp", "lib_type"])) 

                    
def make_extract(start_time, end_time, output_to, db):
    cur = db.cursor(dictionary=True) 

    (users, accounts, others, roots) = xaltanon.gen_aliases.generate_alias_files(cur)
    
    query_params = {
        "xalt_run": "xalt_run",
        "xalt_link":"xalt_link",
        "xalt_object": "xalt_object",
        "join_run_object": "join_run_object",
        "join_link_object": "join_link_object",
        "start_time" : start_time,
        "end_time": end_time
        }
    
    query = ("""
        select 
            *,
            xalt_run.run_id as runid, 
            xalt_run.date as run_date, 
            xalt_run.uuid as runuuid,
            xalt_run.account as run_account,
            xalt_run.syshost as run_syshost,
            xalt_run.exec_type as run_exec_type,
            xalt_run.exit_code as run_exit_code,
            xalt_run.user as run_user,
            xalt_run.exec_path as run_exec_path,
            xalt_run.module_name as run_module_name,
            obj_run.object_path as run_object_path, 
            obj_run.module_name as run_module_name,
            obj_run.timestamp as run_timestamp,
            obj_run.lib_type as run_lib_type
        from {xalt_run}
        left join {join_run_object} 
           on {xalt_run}.run_id={join_run_object}.run_id and {join_run_object}.obj_id > 5304
        left join {xalt_object} as obj_run 
           on {join_run_object}.obj_id=obj_run.obj_id 
        where start_time >= {start_time} and start_time < {end_time} 
      
        ;""").format(**query_params)



    print "Running query"
    print query
    rows = cur.execute(query)
    recs = {}
    
    print "Spooling through rows"
    rowcount = 0
    for row in cur:
        rowcount += 1
        if rowcount % 1000 == 0:
            print "At", rowcount, "th row.", row["start_time"], "\t", row["runid"]
        run_id = row["runid"]
        if run_id not in recs:
            recs[run_id] = dict_slice(row, 
                ["runid", "job_id", "run_uuid", "runuuid", "rundate", 
                 "run_syshost", "run_account", 
                 "run_exec_type", "start_time", "end_time", 
                 "run_time", "num_cores", "num_nodes", 
                 "num_threads", "queue", "run_exit_code", 
                 "user", "run_exec_path", "run_module_name", "cwd"])
            entryToEpoch(recs[run_id],"rundate")
            recs[run_id]["runtime_libs"] = {}
            recs[run_id]["static_libs"] = {}
            
        if row["run_object_path"] is not None:
            recs[run_id]["runtime_libs"][row["run_object_path"]] = dict_slice(row,
                    ["run_object_path", "orun_module_name", "run_timestamp", "run_lib_type"])
            entryToEpoch(recs[run_id]["runtime_libs"][row["run_object_path"]],"run_timestamp")   
    
    print "Requerying to get static links"
    query = ("""
        select 
            *,
            xalt_run.run_id as runid, 
            xalt_run.date as run_date, 
            xalt_run.uuid as runuuid,
            xalt_run.account as run_account,
            xalt_run.syshost as run_syshost,
            xalt_run.exec_type as run_exec_type,
            xalt_run.exit_code as run_exit_code,
            xalt_run.user as run_user,
            xalt_run.exec_path as run_exec_path,
            xalt_run.module_name as run_module_name,
            obj_link.object_path as link_object_path, 
            obj_link.module_name as link_module_name,
            obj_link.timestamp as link_timestamp,
            obj_link.lib_type as link_lib_type
        from {xalt_run}
        left join {xalt_link}
           on {xalt_link}.uuid = {xalt_run}.uuid
        left join {join_link_object} 
           on {join_link_object}.link_id={xalt_link}.link_id and {join_link_object}.obj_id > 5304
        left join {xalt_object} as obj_link
           on obj_link.obj_id = {join_link_object}.obj_id
        where start_time >= {start_time} and start_time < {end_time}  and obj_link.obj_id is not null      
        ;""").format(**query_params)



    print "Running query"
    print query
    rows = cur.execute(query)
    rowcount = 0
    print "Spooling through rows"
    for row in cur:
        rowcount += 1
        if rowcount % 1000 == 0:
            print "At", rowcount, "th row.", row["start_time"], "\t", row["runid"]
        run_id = row["runid"]
        
        recs[run_id]["static_libs"][row["link_object_path"]] = dict_slice(row,
                ["link_object_path", "link_module_name", "olink_timestamp", "link_lib_type"])
        entryToEpoch(recs[run_id]["static_libs"][row["link_object_path"]],"link_timestamp")   

    
    with open(output_to, "w") as f:
        f.write( json.dumps(recs, indent=4))
                    
                    

                                  
if __name__ == '__main__': 
    db = mysql.connector.connect(host="localhost", 
                     user="root", 
                      passwd="",
                      db="xalt")
    import time
    def ymdToEpoch(year, month, day): 
        return int(time.mktime((year,month,day,0,0,0,0,0,0)))
    make_extract(ymdToEpoch(2014,8,1),ymdToEpoch(2014,9,1), 
                 "xalt_dump_{year}_{mo}.json".format(year=2014, mo=8), db)
    
    
    
    
