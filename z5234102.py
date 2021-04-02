import sqlite3
import json
import urllib.request as request
from flask import Flask
from flask_restx import Resource, Api
from flask_restx import fields
from flask_restx import reqparse
from datetime import datetime
import time
import logging
import ctypes

app = Flask(__name__)
api = Api(app, version='1.0', title='Sample API',
    description='A sample API',
)

parser = reqparse.RequestParser()
parser.add_argument('id', type=str, help='Id can not be converted')
parser.add_argument('order_by', type=str )
parser.add_argument('name', type=str )

@api.route('/tv-shows/import')
@api.doc(params={'name': 'TV-show name'})
class ImportShow(Resource):
    @api.response(200, "Successful")
    # @api.response(201, "Created")
    @api.response(400, "Bad request")
    @api.doc(responses={201: 'Created'})
    @api.doc(responses={401: 'TV-show not exist'})
    def post(self):
        args = parser.parse_args()
        name = args.get('name')
        fname = name.replace(' ','%20')
        found = False
        updating = False
        dataSource = request.Request(f"http://api.tvmaze.com/search/shows?q={fname}")
        data=json.loads(request.urlopen(dataSource).read())
        for i in range(len(data)):
            if data[i]["show"]["name"].lower() == name.lower():
                found = True
                res={"request":data[i]["show"]["name"],"name":name}
                updating, ID = Getkey(data[i]["show"]["id"])
                if not updating:
                    ID = ID+1
                InsertTuple(data[i]["show"],ID,updating)
        # print(data.keys())
        if not found:
          return {},401
        else:
            if updating:
                return {},201
            else:
                return {},200



@api.route('/tv-shows/<id>')
@api.doc(params={'id': 'An ID'})
class RetrieveShow(Resource):
    def get(self, order_by):
        return {}

    @api.response(403, 'Not Authorized')
    def post(self, order_by):
        api.abort(403)

def InsertTuple(record,ID,updating):
    print("updating",updating)
    con = sqlite3.connect('z5234102.db')
    c=con.cursor()
    name = record["name"]
    unix = int(time.time())
    update_time = str(datetime.fromtimestamp(unix).strftime('%Y-%m-%d-%H:%M:%S'))
    TVmazeId = record["id"]
    tuple_type = record["type"]
    language = record["language"]
    genres = ','.join(record["genres"])
    Status = record["status"]
    Runtime = record["runtime"]
    Premiered = record["premiered"]
    OfficialSite =record["officialSite"]
    Schedule = json.dumps(record["schedule"])
    Rating = json.dumps(record["rating"])
    Weight = record["weight"]
    Network = json.dumps(record["network"])
    Summary = record["summary"]
    _Links = json.dumps(record["_links"])
    print("type of name:", type(tuple_type),"name", tuple_type)
    print("type of TVmazeId:", type(TVmazeId),"TVmazeId", TVmazeId)
    print("type of update_time:", type(update_time),"update_time", update_time)
    print("type of tuple_type:", type(tuple_type),"tuple_type", tuple_type)
    print("type of language:", type(language),"language", language)
    print("type of genres:", type(genres),"genres", genres)
    print("type of Status:", type(Status),"Status", Status)
    print("type of Runtime:", type(Runtime),"Runtime", Runtime)
    print("type of Premiered:", type(Premiered),"Premiered", Premiered)
    print("type of OfficialSite:", type(OfficialSite),"OfficialSite", OfficialSite)
    print("type of Schedule:", type(Schedule),"Schedule", Schedule)
    print("type of Rating:", type(Rating),"Rating", Rating)
    print("type of Weight:", type(Weight),"Weight", Weight)
    print("type of Network:", type(Network),"Network", Network)
    print("type of Summary:", type(Summary),"Summary", Summary)
    print("type of _Links:", type(_Links),"_Links", _Links)
    print(type(OfficialSite))
    if updating:
      c.execute("UPDATE TVSHOWS SET TVmazeId=?, LastUpdate=? ,Name=?,Type=?,Language=?,Genres=?,Status=?,Runtime=?,Premiered=?,OfficialSite=?,Schedule=?,Rating=?,Weight=?,Network=?,Summary=?,_Links=? WHERE Id=? ", \
        (TVmazeId, update_time, name,tuple_type,language,genres,Status,Runtime,Premiered,OfficialSite,Schedule,Rating,Weight,Network,Summary,_Links,ID))
    else:
      c.execute("INSERT INTO TVSHOWS (Id,TVmazeId,LastUpdate,Name,Type,Language,Genres,Status,Runtime,Premiered,OfficialSite,Schedule,Rating,Weight,Network,Summary,_Links) \
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,? )",(ID,TVmazeId,update_time,name,tuple_type,language,genres,Status,Runtime,Premiered,OfficialSite,Schedule,Rating,Weight,Network,Summary,_Links))
    print ("Table created successfully")
    con.commit()
    con.close()
def Getkey(TVmazeId):
    con = sqlite3.connect('z5234102.db')
    c=con.cursor() 
    TVmazeId = str(TVmazeId)
    cursor = c.execute("SELECT Id, TVmazeId from TVSHOWS ;")
    print("cursor type",type(cursor),"cursor",cursor)
    all_records = cursor.fetchall()
    print("all_records type",type(all_records),"all_records",all_records)
    if len(all_records):
      cursor = c.execute("SELECT Id from TVSHOWS where TVmazeId=?;",(TVmazeId,))
      show_exist = cursor.fetchall()
      print("show_exist type",type(show_exist),"show_exist",show_exist)
      if len(show_exist): # show already exist in database, return Id of the show
          return True,show_exist[0][0]
      else:# show not exist in database, return the max key in database
        cursor = c.execute("SELECT max( Id ) FROM TVSHOWS;")
        ID = cursor.fetchone()[0]
        print("ID type",type(ID),"ID",ID)
        return False,ID
    else:
      return False, 0
    

######################## viewing cursor
    # get_value=ctypes.cast(cursor, ctypes.py_object).value #读取地址中的变量
    # print("cursor value",get_value)
######################## viewing cursor

    # if not len(list(cursor)):
    #   con.commit()
    #   con.close()
    #   return False, 1
    # print("TVmazeId type",type(TVmazeId),"TVmazeId",TVmazeId)
    # cursor = c.execute("SELECT Id from TVSHOWS where TVmazeId=?;",(TVmazeId,))
    # print("cursor",cursor)
    # if len(list(cursor)):
    #     # ID = cursor.fetchone()[0]
    #     result, = cursor.fetchone()
    #     print("result",result)


    # else:
    #   cursor = c.execute("SELECT max( Id ) FROM TVSHOWS;")
    #   result, = cursor.fetchone()
    #   print("result",result)
    #   # ID = cursor.fetchone()[0]
    # con.commit()
    # con.close()
    return False, 3
if __name__ == '__main__':
    con = sqlite3.connect('z5234102.db')
    c=con.cursor()
    c.execute('''CREATE TABLE  IF NOT EXISTS TVSHOWS
        (Id INT PRIMARY KEY     NOT NULL,
        TvmazeId      INT,
        LastUpdate    TEXT,
        Name          TEXT,
        Type          TEXT,
        Language      TEXT,
        Genres        TEXT,
        Status        TEXT,
        Runtime       INT,
        Premiered     TEXT,
        OfficialSite  TEXT,
        Schedule      TEXT,
        Rating        TEXT,
        Weight        INT,
        Network       TEXT,
        Summary       TEXT,
        _Links        TEXT);''')
    print ("Table created successfully")
    con.commit()
    con.close()

    app.run(debug=True)
    # http://api.tvmaze.com/shows
    # dataSource = request.Request("http://api.tvmaze.com/shows")
    # data=json.loads(request.urlopen(dataSource).read())
    # print(type(data))
    # print(type(data[1]))
    # print(data[1])

    print("start")