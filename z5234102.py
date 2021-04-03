import sqlite3
import json
import urllib.request as request
from flask import Flask
from flask_restx import Resource, Api
from flask_restx import fields
from flask_restx import reqparse
from flask_restx import Model
from flask_restx import Response
from flask import send_file
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
# from  matplotlib import cm
import time
import logging
import ctypes

app = Flask(__name__)
api = Api(app, version='1.0', title='TV-MAZE API',
    description='A REST API build on TVMAZE API ',
)
port_number = 8000
host = 'localhost'
tv_model = api.model('TV',{'name':fields.Integer,
  'name':fields.String,
  'type':fields.String,
  'language':fields.String,
  'genres':fields.List(fields.String),
  'status':fields.String,
  'runtime':fields.Integer,
  'premiered':fields.String,
  'officialSite':fields.String,
  'schedule':fields.Raw,
  'rating':fields.Raw,
  'weight':fields.Integer,
  'network':fields.Raw,
  'summary':fields.String 
})

# order_model = api.model('TV',{
#     'id':fields.Integer,
#     'name':fields.String,
#     'runtime':fields.Integer,
#     'premiered':fields.String,
#     'rating-average':fields.Integer})
# filter_model = api.model('TV',{
#     'tvmaze_id':fields.Integer,
#     'id':fields.Integer,
#     'last-update':fields.String,
#     'name':fields.String,
#     'type': fields.String,
#     'language':fields.String,
#     'genres':fields.List(fields.String),
#     'status':fields.String,
#     'runtime':fields.Integer,
#     'premiered':fields.String,
#     'officialSite':fields.String,
#     'schedule':fields.Raw,
#     'rating':fields.Raw,
#     'weight':fields.Integer,
#     'network':fields.Raw,
#     'summary':fields.String 
# })
parser = reqparse.RequestParser()
parser.add_argument('name', type=str, location='args' )
parser.add_argument('order_by',type=str, location='args' )
parser.add_argument('page', type=int , location='args')
parser.add_argument('page_size', type=int , location='args')
parser.add_argument('filter', type=str, location='args' )
parser.add_argument('format', type=str, location='args' )
parser.add_argument('by', type=str, location='args' )

##########################################  Q1 ########################################
@api.route('/tv-shows/import')
@api.doc(params={'name': 'title for the tv show'})
class ImportShow(Resource):
    @api.response(201, "Created")
    @api.response(404, "TV-show not found")
    def post(self):
        args = parser.parse_args()
        name = args.get('name')
        fname = name.replace(' ','%20')
        found = False
        updating = False
        dataSource = request.Request(f"http://api.tvmaze.com/search/shows?q={fname}")
        data=json.loads(request.urlopen(dataSource).read())
        for i in range(len(data)):
            if data[i]["show"]["name"].lower() == ' '.join(name.split()).lower():
                found = True
                res={"request":data[i]["show"]["name"],"name":name}
                updating, ID = Getkey(data[i]["show"]["id"])
                if not updating:
                    ID = ID+1
                result = InsertTuple(data[i]["show"],ID,updating)
        if not found:
          return {"message": "TVshow name: {} does not exist".format(name)},404
        else:
            return result,201

##########################################  Q2 ########################################

@api.route('/tv-shows/<id>')
@api.doc(params={'id': 'An ID'})
class RetrieveShow(Resource):
    @api.response(200, "OK")
    @api.response(404, "TV-show not found")
    def get(self,id):
        result = GetRecord(id)
        if result:
            return result,200
        else:
            return {"messge":"TVshow id: {} does not exist".format(id)},404

##########################################  Q3 ########################################
    @api.response(200, "OK")
    @api.response(404, "TV-show not found")
    def delete(self,id):
        con = sqlite3.connect('z5234102.db')
        c=con.cursor()
        show_exist = c.execute("SELECT Id from TVSHOWS where Id=?;",(id,)).fetchall()
        if len(show_exist):
            DeleteRecord(id)
            return { 
                "message" :"The tv show with id {} was removed from the database!".format(id),
                "id": id
            },200
        else:
          return {"messge":"TVshow id: {} does not exist".format(id)},404

##########################################  Q4 ########################################
    @api.response(201, "Created")
    @api.response(404, "TV-show not found")
    @api.expect(tv_model,validate=True)
    @api.doc(body=tv_model)
    def patch(self,id):
        payload = api.payload
        print(type(api.payload))
        print(payload.keys)
        con = sqlite3.connect('z5234102.db')
        c=con.cursor()
        unix = int(time.time())
        update_time = str(datetime.fromtimestamp(unix).strftime('%Y-%m-%d-%H:%M:%S'))
        show_exist = c.execute("SELECT Id from TVSHOWS where Id=?;",(id,)).fetchall()
        if len(show_exist):
          c.execute("UPDATE TVSHOWS set LastUpdate = ? where Id=?;",( update_time, id))
          i = 0
          for key, value in payload.items():
              print(key, value)
              if key == 'genres':
                  value = ','.join(value)
              if key == 'schedule' or key == 'rating' or key == 'network':
                  value = json.dumps(value)

              print(type(key),type(value))
              print(i+1)
              c.execute("UPDATE TVSHOWS set "+key+" = ? where Id=?;",(value,id,))
              time.sleep(0.01)
          con.commit()
          con.close()
          return {  
              "id" : id,  
              "last-update": update_time,
              "_links": {
                  "self": {
                    "href": "http://{}:{}/tv-shows/{}".format(host,port_number,id)
                  }
              }
          },201
        else:
            return {"message": "TVshow id: {} does not exist".format(id)},404


##########################################  Q5 ########################################
@api.route('/tv-shows')
@api.doc(params={'order_by': 'comma separated string value to sort the list based on the given criteria\n(+ ascending,-descending)'})
@api.doc(params={'page': 'page number will be shown'})
@api.doc(params={'page_size': 'the number of TV shows per page'})
@api.doc(params={'filter': 'attributes will be shown'})
class OrderShow(Resource):
    @api.response(200, "OK")
    @api.response(404, "TV-show not found")
    @api.response(400, "Bad Request")
    def get(self):
        order_by = parser.parse_args().get('order_by')
        filter = parser.parse_args().get('filter')
        page = parser.parse_args().get('page')
        page_size = parser.parse_args().get('page_size')
        order_range = ['id','name','runtime','premiered','rating-average']
        filter_range = ['tvmaze_id','id' ,'last-update' ,'name','type ','language' ,'genres' ,'status' ,'runtime' ,'premiered ','officialSite' ,'schedule' ,'rating ','weight' ,'network' ,'summary']
        print(order_by)
        print(filter)
        print(page)
        print(page_size)
        filter_list = []
        order_list = []
        order_dic = {}
        if filter:
          print(type(filter.split(',')),filter.split(','))
          for filter_item in filter.split(','):
              print(filter_item)
              if filter_item not in filter_range:
                print("filter_item",filter_item)
                return {
                    "errors": {
                      "filter": "The value '{}' is not a valid choice for 'filter'.".format(filter_item)
                    },
                    "message": "Input payload validation failed"
                  },400
              else:
                filter_list.append(filter_item)
        else :
            filter_list = ['id','name']
        if order_by:
          for order_item in order_by.split(','):
              if order_item[0] == "+":
                  order_list.append(order_item[1:]+' ASC')
                  # order_dic[order_item[1:]] = 'ASC'
              elif order_item[0] == "-":
                  order_list.append(order_item[1:]+' DESC')
                  # order_dic[order_item[1:]] = 'DESC'
              else:
                  return {
                      "errors": {
                        "order_by": "The value should start with '+' or '-'."
                      },
                      "message": "Input payload validation failed"
                  },400
              if order_item[1:] not in order_range:
                  return {
                  "errors": {
                    "order_by": "The value '{}' is not a valid choice for 'order_by'.".format(order_item)
                  },
                  "message": "Input payload validation failed"
                },400
        else:
          order_list = ['id']
          order_dic['id'] = 'ASC'
        print(order_list)
        print(order_dic)
        print(filter_list)
        # order = ' '.join()
        con = sqlite3.connect('z5234102.db')
        c=con.cursor()
        print(' '.join(filter_list))
        print( ','.join(order_list))
        ordered = c.execute("SELECT " + ','.join(filter_list)+" from TVSHOWS ORDER BY " + ','.join(order_list)+";").fetchall()
        con.commit()
        con.close()
        print(ordered)
        show_dic = {}
        result_list = []
        left_tuple = len(ordered)
        p = 1
        while left_tuple > page_size:
            print("p",p)
            show_list = []
            # print("page_size+page_size*(p-1)",page_size+page_size*(p-1))
            for i in range(page_size*(p-1),page_size+page_size*(p-1)):
              # print("i = ",i )
              for j in range(len(filter_list)):
                  show_dic[filter_list[j]] = ordered[i][j]
              show_list.append(show_dic)
              print(show_dic)
              show_dic = {}
            result = {
                "page": p,
                "page-size": page_size,
                "tv-shows": show_list,
            }
            result_list.append(result)
            left_tuple = left_tuple - page_size
            p +=1
        if left_tuple > 0:
            show_list = []
            for i in range(page_size*(p-1),len(ordered)):
              for j in range(len(filter_list)):
                  show_dic[filter_list[j]] = ordered[i][j]
              show_list.append(show_dic)
              print(show_dic)
              show_dic = {}
            result = {
                "page": p,
                "page-size": page_size,
                "tv-shows": show_list,
            }
            result_list.append(result)
        else:
            p -= 1
        final_result = result_list[page-1]
        print("init_final_result",final_result)
        print("final p=",p)
        if page > 1  and page < p:
            print("page > 1  and page < p")
            final_result["_links"] = {
                "self": {
                  "href": "http://{}:{}/tv-shows?order_by={}&page={}&page_size=1000&filter={}".format(host,port_number,order_by,page,page_size,filter)
                },
                "previous": {
                  "href": "http://{}:{}/tv-shows?order_by=+id&page={}&page_size=1000&filter=id,name".format(host,port_number,order_by,page-1,page_size,filter)
                },
                "next": {
                  "href": "http://{}:{}/tv-shows?order_by=+id&page={}&page_size=1000&filter=id,name".format(host,port_number,order_by,page+1,page_size,filter)
                }
              }
            return final_result,200
        elif page == 1 and p>1:
            final_result["_links"] = {
                "self": {
                  "href": "http://{}:{}/tv-shows?order_by={}&page={}&page_size=1000&filter={}".format(host,host,port_number,order_by,page,page_size,filter)
                },
                "next": {
                  "href": "http://{}:{}/tv-shows?order_by=+id&page={}&page_size=1000&filter=id,name".format(host,port_number,order_by,page+1,page_size,filter)
                }
              }
            return final_result,200
        elif page >1 and page == p:
            final_result["_links"] = {
                "self": {
                  "href": "http://{}:{}/tv-shows?order_by={}&page={}&page_size=1000&filter={}".format(host,port_number,order_by,page,page_size,filter)
                },
                "previous": {
                  "href": "http://{}:{}/tv-shows?order_by=+id&page={}&page_size=1000&filter=id,name".format(host,port_number,order_by,page-1,page_size,filter)
                }
              }
            return final_result,200
        elif page ==1 and p ==1:
            final_result["_links"] = {
                "self": {
                  "href": "http://{}:{}/tv-shows?order_by={}&page={}&page_size=1000&filter={}".format(host,port_number,order_by,page,page_size,filter)
                },
              }
            return final_result,200
        else:
            return {"message": "invalid page number"},400
        
        # print(result_list)

##########################################  Q6 ########################################
@api.route('/tv-shows/statistics')
@api.doc(params={'format': 'json or image'})
@api.doc(params={'by': 'attributes of TVshow'})
class ShowStatistics(Resource):
    @api.response(200, "OK")
    @api.response(404, "Atrribute not found")
    @api.response(400, "Bad Request")
    def get(self):
        format = parser.parse_args().get('format')
        by = parser.parse_args().get('by')
        con = sqlite3.connect('z5234102.db')
        c = con.cursor()
        types = []
        res = c.execute("SELECT distinct "+by+" from TVSHOWS;").fetchall()
        for i in range(len(res)):
            types.append(res[i][0])
        lists = []
        update_time= []
        res = c.execute("SELECT "+by+", lastUpdate from TVSHOWS;").fetchall()
        for i in range(len(res)):
            lists.append(res[i][0]) 
            update_time.append(res[i][1])
        print(update_time)
        t = time.time()
        print(t)
        count = 0
        for str_time in update_time:
            time_stamp = time.mktime(time.strptime(str_time,'%Y-%m-%d-%H:%M:%S'))
            if t - time_stamp < 172800:
              count += 1
            print(time_stamp)
        json_dic = {}
        pie_dic = {}
        for i in range(len(types)):
          json_dic[types[i]] = '%.1f%%' % (round(lists.count(types[i])/len(lists),3)*100)
          pie_dic[types[i]] = round(lists.count(types[i])/len(lists),3*100)
        if format == "json":
            result = { 
                "total": len(lists),
                "total-updated": count,
                "values" : json_dic
              }
            print(json_dic)
            print(lists)
            con.commit()
            con.close()
            print(by)
            print(format)

            return result,200
        else:
            data = {}
            labels = list(pie_dic.keys())
            sizes = list(pie_dic.values())
            print(type(labels))
            print(sizes)
            data["type"] = list(pie_dic.keys())
            data["value"] = list(pie_dic.values())
            # labels = list(pie_dic.keys())
            # sizes = list(pie_dic.values())
            print('data',data)
            df = pd.DataFrame.from_dict(data)
            df = df.set_index('type')
            unival = df['value'].value_counts()
            unival.plot.pie(subplots=True)
            # plt.show()
          # plt.figure(figsize=(6,9))

          # colors = ['red','yellowgreen']
          # explode = (0,0)
          # patches,text1,text2 = plt.pie(sizes,
          #             labels=labels,
          #             explode=explode,
          #             autopct = '%3.2f%%', 
          #             shadow = False, 
          #             colors=colors,
          #             startangle =90,
          #             pctdistance = 0.6)
          # plt.axis('equal')
          # plt.show()
          # fig, ax = plt.subplots(figsize=(6,6))
          # colors = cm.rainbow(np.arange(len(sizes))/len(sizes))
          # ax.axis('equal')  
          # ax.set_title(''.format(by), loc='left')

            plt.savefig('Ass2.jpg')
            # plt.show()
            filename = 'Ass2.jpg'
            # send_file(filename,mimetype='image/jpg')
            resp = Response(filename, mimetype="image/jpeg")
            return resp

###########################################################
#            self defined functions
# InsertTuple(record,ID,updating): insert imported TV shows into database
# Getkey(TVmazeId): get the apropriate ID for the insert record 
# GetRecord(ID): get all info for an specific ID 
# GetPreID(ID): find previous ID
# GetPreID(ID): find next ID
# DeleteRecord(ID): delete record where Id = ID
###########################################################
def InsertTuple(record,ID,updating):
    print("updating",updating)
    con = sqlite3.connect('z5234102.db')
    c=con.cursor()
    name = record["name"]
    unix = int(time.time())
    update_time = str(datetime.fromtimestamp(unix).strftime('%Y-%m-%d-%H:%M:%S'))
    tvmaze_Id  = record["id"]
    tuple_type = record["type"]
    language = record["language"]
    genres = ','.join(record["genres"])
    status = record["status"]
    runtime = record["runtime"]
    premiered = record["premiered"]
    officialSite =record["officialSite"]
    schedule = json.dumps(record["schedule"])
    rating = json.dumps(record["rating"])
    weight = record["weight"]
    network = json.dumps(record["network"])
    summary = record["summary"]
    _links = json.dumps(record["_links"])
    rating_average = record["rating"]["average"]
    # print("type of name:", type(tuple_type),"name", tuple_type)
    # print("type of TVmazeId:", type(TVmazeId),"TVmazeId", TVmazeId)
    # print("type of update_time:", type(update_time),"update_time", update_time)
    # print("type of tuple_type:", type(tuple_type),"tuple_type", tuple_type)
    # print("type of language:", type(language),"language", language)
    # print("type of genres:", type(genres),"genres", genres)
    # print("type of Status:", type(Status),"Status", Status)
    # print("type of Runtime:", type(Runtime),"Runtime", Runtime)
    # print("type of Premiered:", type(Premiered),"Premiered", Premiered)
    # print("type of OfficialSite:", type(OfficialSite),"OfficialSite", OfficialSite)
    # print("type of Schedule:", type(Schedule),"Schedule", Schedule)
    # print("type of Rating:", type(Rating),"Rating", Rating)
    # print("type of Weight:", type(Weight),"Weight", Weight)
    # print("type of Network:", type(Network),"Network", Network)
    # print("type of Summary:", type(Summary),"Summary", Summary)
    # print("type of _Links:", type(_Links),"_Links", _Links)
    # print("Rating_average:", type(Rating_average),"Rating_average",Rating_average)

    # print(type(OfficialSite))
    if updating:
      c.execute("UPDATE TVSHOWS SET tvmaze_Id =?, lastUpdate=? ,name=?,type=?,language=?,genres=?,status=?,runtime=?,premiered=?,officialSite=?,schedule=?,rating=?,weight=?,network=?,summary=?,_links=?,rating_average=? WHERE Id=? ", \
        (TVmazeId, update_time, name,tuple_type,language,genres,Status,Runtime,Premiered,OfficialSite,Schedule,Rating,Weight,Network,Summary,_Links,ID,rating_average))
    else:
      c.execute("INSERT INTO TVSHOWS (Id,tvmaze_Id ,lastUpdate,name,type,language,genres,status,runtime,premiered,officialSite,schedule,rating,weight,network,summary,_links,rating_average) \
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,? )",(ID,Tvmaze_Id ,update_time,name,tuple_type,language,genres,Status,Runtime,Premiered,OfficialSite,Schedule,Rating,Weight,Network,Summary,_Links,rating_average))
    print ("Table created successfully")
    con.commit()
    con.close()
    result = { 
              "id" : ID,  
              "last-update": update_time,
              "tvmaze-id" : TVmazeId,
              "_links": {
                  "self": {
                    "href": "http://{}:{}/tv-shows/{}".format(host,port_number,ID)
                  }
              } 
            }
    return result

def Getkey(TVmazeId):
    con = sqlite3.connect('z5234102.db')
    c = con.cursor() 
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
          con.commit()
          con.close()
          return True,show_exist[0][0]
      else:# show not exist in database, return the max key in database
        cursor = c.execute("SELECT max( Id ) FROM TVSHOWS;")
        ID = cursor.fetchone()[0]
        print("ID type",type(ID),"ID",ID)
        con.commit()
        con.close()
        return False,ID
    else:
        con.commit()
        con.close()
        return False, 0

def GetRecord(ID):
    con = sqlite3.connect('z5234102.db')
    c = con.cursor()
    show_exist = c.execute("SELECT * from TVSHOWS where Id=?;",(ID,)).fetchall()
    con.commit()
    con.close()
    if len(show_exist):
        preId = GetPreID(ID)
        nextId = GetNextID(ID)
        result = {  
            "tvmaze-id" :show_exist[0][1],
            "id": show_exist[0][0],
            "last-update": show_exist[0][2],
            "name": show_exist[0][3],
            "type": show_exist[0][4],
            "language":show_exist[0][5],
            "genres": show_exist[0][6].split(','),
            "status": show_exist[0][7],
            "runtime": show_exist[0][8],
            "premiered": show_exist[0][9],
            "officialSite": show_exist[0][10],
            "schedule":  json.loads(show_exist[0][11]),
            "rating": json.loads(show_exist[0][12]),
            "weight":  show_exist[0][13],
            "network": json.loads( show_exist[0][14]),
            "summary": show_exist[0][15]
        }
        if preId and nextId:
            previous = "http://{}:{}/tv-shows/{}".format(host,port_number,preId)
            nxt = "http://{}:{}/tv-shows/{}".format(host,port_number,nextId)
            result["_links"] = {
                  "self": {
                    "href": "http://{}:{}/tv-shows/{}".format(host,port_number,ID)
                  },
                  "previous": {
                    "href": previous
                  },
                  "next": {
                    "href": nxt
                  }
                } 
        elif preId:
            previous = "http://{}:{}/tv-shows/{}".format(host,port_number,preId)
            result["_links"] = {
                  "self": {
                    "href": "http://{}:{}/tv-shows/{}".format(host,port_number,ID)
                  },
                  "previous": {
                    "href": previous
                  },
                } 
        elif nextId:
            nxt = "http://{}:{}/tv-shows/{}".format(host,port_number,nextId)
            result["_links"] = {
                  "self": {
                    "href": "http://{}:{}/tv-shows/{}".format(host,port_number,ID)
                  },
                  "next": {
                    "href": nxt
                  }
                } 
        else:
            result["_links"] = {
                  "self": {
                    "href": "http://{}:{}/tv-shows/{}".format(host,port_number,ID)
                  }
                } 
        return result
    else:
      return {}

def GetPreID(ID):
  ID = int(ID)
  pre = ID-1
  con = sqlite3.connect('z5234102.db')
  c=con.cursor()   
  while True:
      print("pre while")
      cursor = c.execute("SELECT Id from TVSHOWS where Id=?;",(pre,))
      show_exist = cursor.fetchall()
      if len(show_exist):
          con.commit()
          con.close()
          return pre
      else:
          print(pre)
          pre = pre-1
          if pre == 0:          
              con.commit()
              con.close()
              return 0

def GetNextID(ID):
  ID = int(ID)
  nxt = ID+1
  con = sqlite3.connect('z5234102.db')
  c=con.cursor()   
  maxId = c.execute("SELECT max( Id ) FROM TVSHOWS;").fetchone()[0]
  while True:
      print("next while")
      show_exist = c.execute("SELECT Id from TVSHOWS where Id=?;",(nxt,)).fetchall()
      if len(show_exist):
          con.commit()
          con.close()
          return nxt
      else:
          nxt = nxt+1
          if nxt > maxId:          
              con.commit()
              con.close()
              return 0

def DeleteRecord(ID):
    con = sqlite3.connect('z5234102.db')
    c = con.cursor()
    c.execute("DELETE from TVSHOWS where ID=?;",(ID,))
    con.commit()
    con.close()
if __name__ == '__main__':
    con = sqlite3.connect('z5234102.db')
    c=con.cursor()
    c.execute('''CREATE TABLE  IF NOT EXISTS TVSHOWS
        (Id INT PRIMARY KEY     NOT NULL,
        tvmaze_Id      INT,
        lastUpdate    TEXT,
        name          TEXT,
        type          TEXT,
        language      TEXT,
        genres        TEXT,
        status        TEXT,
        runtime       INT,
        premiered     TEXT,
        officialSite  TEXT,
        schedule      JSON,
        rating        JSON,
        weight        INT,
        network       TEXT,
        summary       TEXT,
        _links        TEXT,
      rating_average REAL);''')
    print ("Table created successfully")
    con.commit()
    con.close()

    app.run(host = host,
        port = port_number,  
        debug = True) 