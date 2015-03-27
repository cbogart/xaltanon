import MySQLdb
import _mysql_exceptions
import json
import re
import os
import pdb


# Format is: username -> { "id": numeric id (as string of digits), "numalias": random unique string of digits, "namealias": random unique string of letters }

class DuplicateIdForName(Exception):
    pass
class NoUserInPath(Exception):
    pass
class UserUnknown(Exception):
    pass

class Aliases:
    def __init__(self, prefix):
        self.aliases={}
        self.prefix = prefix;
        self.numid = 1;
    def add_name(self, name):
        self.aliases[name] = { "numalias": self.numid }
        self.numid += 1
    def add_name_id(self, name, numid):
        if name not in self.aliases: 
            self.add_name(name)
        if "id" in self.aliases[name] and self.aliases[name]["id"] != numid:
            raise DuplicateIdForName("Inconsistency: ", name, "has", self.aliases[name]["id"], "and", numid)
        self.aliases[name]["id"] = numid
    def anonymize(self, field):
        try:
            return self.anonymize_username(field)
        except UserUnknown:
            pass
        try:
            return self.anonymize_path(field)
        except NoUserInPath:
            pass
        except UserUnknown:
            pass
        return field

    def anonymize_username(self, name):
        if name in self.aliases:
            return self.prefix + str(self.aliases[name]["numalias"])
        else:
            raise UserUnknown("Couldn't find " + name)
    def anonymize_path(self, path):
        try:
            (root,idnum,name) = find_root_id_name(path)
            newpath = path. \
                 replace(idnum, str(self.aliases[name]["numalias"])). \
                 replace(name, self.prefix + str(self.aliases[name]["numalias"]))
            return newpath
        except NoUserInPath, ne:
            return path
        
users = Aliases("U")
accounts = Aliases("A")
others = set()
roots = set()

db = MySQLdb.connect(host="localhost", # your host, usually localhost
                     user="root", # your username
                      passwd="", # your password
                      db="xalt") # name of the data base
cur = db.cursor() 

def find_root_id_name(path):
    parts = [p for p in path.split("/") if len(p) > 0]
    if len(parts) >= 3 and re.match(r"\d+", parts[1]):
        return (parts[0],parts[1],parts[2])
    raise NoUserInPath()

def find_users_in_path_column(table, field, users):
    print table + "." + field
    cur.execute("select " + field + " from " + table)
    for row in cur.fetchall():
        try:
            (root,idnum,name) = find_root_id_name(row[0])
            roots.add(root)
            users.add_name_id(name, idnum)
        except DuplicateIdForName, d:
            print str(d)
        except NoUserInPath:
            others.add(row[0])

def find_users_in_user_column(table, field, users):
    print table + "." + field
    cur.execute("select distinct(" + field + ") from " + table + " order by rand()")
    for row in cur.fetchall():
        users.add_name(row[0])

class NiceCursorDescription:
    def __init__(self, cursor):
        self.desc = cursor.description
    def posn_of(self, fldname):
        return self.field_list().index(fldname)
    def field_list(self):
        return [d[0] for d in self.desc]
    def gen_insert_stmt(self, newtable):
        return "insert into " + newtable + " (" + ", ".join(self.field_list()) + ") " + \
               "values (" + ",".join(["%s" for f in self.field_list()]) + ")"
    

def anonymize_fields(users, accounts, table, fields):
    print table + ".(" + ",".join(fields) + ")"

    anontable = table + "_anon";
    cur_insert = db.cursor()
    try:
        cur_insert.execute("drop table " + anontable)
    except _mysql_exceptions.OperationalError:
        pass
    cur_insert.execute("create table " + anontable + " as (select * from " + table + " where 1=2)")

    cur.execute("select count(*) from " + table)
    rowcount = cur.fetchone()[0]

    cur.execute("select * from " + table)
    cursor_inf = NiceCursorDescription(cur)
    posns = [cursor_inf.field_list().index(f) for f in fields]
    inserter = cursor_inf.gen_insert_stmt(anontable)

    for (i,row) in enumerate(cur.fetchall()):
        if i%1000 == 0:
            print "Anonymizing row",i,"of",rowcount,"in table",table
            db.commit()
        newrow = list(row)
        for posn in posns:
            newrow[posn] = users.anonymize(row[posn])
            if row[posn] == newrow[posn]:
                try:
                    newrow[posn] = accounts.anonymize(row[posn])
                except Exception, e:
                    print str(row), str(e)
                    pass
        cur_insert.execute(inserter, tuple(newrow))
    db.commit()
        
    

if (os.path.exists("users.json")):
    with open("users.json") as f:
        users.aliases = json.load(f)
    with open("accounts.json") as f:
        accounts.aliases = json.load(f)
else:
    print "Building alias table\n"
    find_users_in_user_column("XALT_RUN", "user", users)
    find_users_in_path_column("XALT_RUN","exec_path", users)
    find_users_in_user_column("XALT_RUN", "account", accounts)
    find_users_in_path_column("XALT_RUN","cwd", users)
    find_users_in_path_column("XALT_LINK","exec_path", users)
    find_users_in_user_column("XALT_LINK", "build_user", users)
    find_users_in_path_column("XALT_OBJECT","object_path", users)
    with open("users.json", "w") as f:
        f.write(json.dumps(users.aliases, indent=4))
    with open("accounts.json", "w") as f:
        f.write(json.dumps(accounts.aliases, indent=4))
        
print "Replacing usernames with aliases\n"
anonymize_fields(users, accounts, "xalt_run", ["user", "cwd", "exec_path", "account"])
anonymize_fields(users, accounts, "xalt_link", ["exec_path", "build_user"])
anonymize_fields(users, accounts, "xalt_object", ["object_path"])

