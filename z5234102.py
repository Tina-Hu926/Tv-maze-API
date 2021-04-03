import sqlite3
import json
import urllib.request as request
from flask import Flask
from flask_restx import Resource, Api
from flask_restx import fields
from flask_restx import reqparse
from flask_restx import Model
from flask import Response
from flask import send_file
from datetime import datetime
import pandas as pd
from matplotlib import pyplot as plt 
import time

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
          return {
                    "errors": {
                      "name": "The value '{}' is not a valid choice for 'name'.".format(name)
                    },
                    "message": "Input parameters validation failed"
                  },404
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
          return {
                    "errors": {
                      "id": "The value '{}' is not a valid choice for 'id'.".format(id)
                    },
                    "message": "Input parameters validation failed"
                  },404

##########################################  Q4 ########################################
    @api.response(201, "Created")
    @api.response(404, "TV-show not found")
    @api.expect(tv_model,validate=True)
    @api.doc(body=tv_model)
    def patch(self,id):
        payload = api.payload
        con = sqlite3.connect('z5234102.db')
        c=con.cursor()
        unix = int(time.time())
        update_time = str(datetime.fromtimestamp(unix).strftime('%Y-%m-%d-%H:%M:%S'))
        show_exist = c.execute("SELECT Id from TVSHOWS where Id=?;",(id,)).fetchall()
        if len(show_exist):
          c.execute("UPDATE TVSHOWS set last_update = ? where Id=?;",( update_time, id))
          i = 0
          for key, value in payload.items():
              if key == 'genres':
                  value = ','.join(value)
              if key == 'schedule' or key == 'rating' or key == 'network':
                  value = json.dumps(value)
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
            return {
                    "errors": {
                      "id": "The value '{}' is not a valid choice for 'id'.".format(id)
                    },
                    "message": "Input parameters validation failed"
                  },404

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
        filter_list = []
        order_list = []
        order_dic = {}
        if filter:
          for filter_item in filter.split(','):
              if filter_item not in filter_range:
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
        con = sqlite3.connect('z5234102.db')
        c=con.cursor()
        ordered = c.execute("SELECT " + ','.join(filter_list)+" from TVSHOWS ORDER BY " + ','.join(order_list)+";").fetchall()
        con.commit()
        con.close()
        show_dic = {}
        result_list = []
        left_tuple = len(ordered)
        p = 1
        while left_tuple > page_size:
            show_list = []
            for i in range(page_size*(p-1),page_size+page_size*(p-1)):
              for j in range(len(filter_list)):
                  show_dic[filter_list[j]] = ordered[i][j]
              show_list.append(show_dic)
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
              show_dic = {}
            result = {
                "page": p,
                "page-size": page_size,
                "tv-shows": show_list,
            }
            result_list.append(result)
        else:
            p -= 1
        if page > 0 and page <= p:
            final_result = result_list[page-1]
        else:
            return {
                    "errors": {
                      "page": "The value '{}' is not a valid choice for 'page'.".format(page)
                    },
                    "message": "Input parameters validation failed"
                  },400
        if page > 1  and page < p:
            final_result["_links"] = {
                "self": {
                  "href": "http://{}:{}/tv-shows?order_by={}&page={}&page_size={}&filter={}".format(host,port_number,order_by,page,page_size,filter)
                },
                "previous": {
                  "href": "http://{}:{}/tv-shows?order_by={}&page={}&page_size={}&filter={},name".format(host,port_number,order_by,page-1,page_size,filter)
                },
                "next": {
                  "href": "http://{}:{}/tv-shows?order_by={}&page={}&page_size={}&filter={},name".format(host,port_number,order_by,page+1,page_size,filter)
                }
              }
            return final_result,200
        if page == 1 and p>1:
            final_result["_links"] = {
                "self": {
                  "href": "http://{}:{}/tv-shows?order_by={}&page={}&page_size={}&filter={}".format(host,port_number,order_by,page,page_size,filter)
                },
                "next": {
                  "href": "http://{}:{}/tv-shows?order_by={}&page={}&page_size={}&filter={}".format(host,port_number,order_by,page+1,page_size,filter)
                }
              }
            return final_result,200
        if page >1 and page == p:
            final_result["_links"] = {
                "self": {
                  "href": "http://{}:{}/tv-shows?order_by={}&page={}&page_size={}&filter={}".format(host,port_number,order_by,page,page_size,filter)
                },
                "previous": {
                  "href": "http://{}:{}/tv-shows?order_by={}&page={}&page_size={}&filter={},name".format(host,port_number,order_by,page-1,page_size,filter)
                }
              }
            return final_result,200
        if page ==1 and p ==1:
            final_result["_links"] = {
                "self": {
                  "href": "http://{}:{}/tv-shows?order_by={}&page={}&page_size={}&filter={}".format(host,port_number,order_by,page,page_size,filter)
                },
              }
            return final_result,200
        # else:
        #     return {
        #             "errors": {
        #               "page": "The value '{}' is not a valid choice for 'page'.".format(page)
        #             },
        #             "message": "Input parameters validation failed"
        #           },400

##########################################  Q6 ########################################
@api.route('/tv-shows/statistics')
@api.doc(params={'format': 'json or image'})
@api.doc(params={'by': 'attributes of TVshow(language,genres,status,type)'})
class ShowStatistics(Resource):
    @api.response(200, "OK")
    @api.response(404, "Atrribute not found")
    @api.response(400, "Bad Request")
    def get(self):
        format = parser.parse_args().get('format')
        by = parser.parse_args().get('by')
        valid_by = ['language','genres','status','type']
        if by not in valid_by:
          return {
                  "errors": {
                    "by": "The value '{}' is not a valid choice for 'by'.".format(by)
                  }
                  },400
        con = sqlite3.connect('z5234102.db')
        c = con.cursor()
        lists = []
        update_time= []
        res = c.execute("SELECT "+by+", last_update from TVSHOWS;").fetchall()
        for i in range(len(res)):
            update_time.append(res[i][1])
            for j in range(len(res[i][0].split(','))):
                lists.append(res[i][0].split(',')[j])
        types =list(set(lists))
        t = time.time()
        count = 0
        for str_time in update_time:
            time_stamp = time.mktime(time.strptime(str_time,'%Y-%m-%d-%H:%M:%S'))
            if t - time_stamp < 172800:
              count += 1
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
            con.commit()
            con.close()
            return result,200
        elif format == "image":
            plt.figure(figsize=(9,12))
            labels = list(pie_dic.keys())
            sizes = list(pie_dic.values())
            colors = ['tomato','navajowhite', 'lightskyblue', 'pink','violet', 'yellowgreen','lightgrey','sandybrown']
            colors = colors[:len(labels)]
            explode = (0.03,)*len(labels)
            patches,l_text,p_text = plt.pie(sizes,explode=explode,labels=labels,colors=colors,
                                            labeldistance = 1.05,autopct = '%3.1f%%',shadow = False,
                                            startangle = 90,pctdistance = 0.6)
            plt.axis('equal')
            plt.title(by+' of TVshows')
            plt.legend(loc='best')
            plt.savefig('Ass2.jpg')
            filename = './Ass2.jpg'
            with open(filename,'rb') as f:
                image = f.read()
            result = Response(image, mimetype="image/jpg")
            return result
        else:
          return{
                  "errors": {
                    "format": "The value '{}' is not a valid choice for 'format'.".format(format)
                  }
                  },400
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
    if updating:
      c.execute("UPDATE TVSHOWS SET tvmaze_Id =?, last_update=? ,name=?,type=?,language=?,genres=?,status=?,runtime=?,premiered=?,officialSite=?,schedule=?,rating=?,weight=?,network=?,summary=?,_links=?,rating_average=? WHERE Id=? ", \
        (tvmaze_Id, update_time, name,tuple_type,language,genres,status,runtime,premiered,officialSite,schedule,rating,weight,network,summary,_links,ID,rating_average))
    else:
      c.execute("INSERT INTO TVSHOWS (Id,tvmaze_Id ,last_update,name,type,language,genres,status,runtime,premiered,officialSite,schedule,rating,weight,network,summary,_links,rating_average) \
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,? )",(ID,tvmaze_Id ,update_time,name,tuple_type,language,genres,status,runtime,premiered,officialSite,schedule,rating,weight,network,summary,_links,rating_average))
    con.commit()
    con.close()
    result = { 
              "id" : ID,  
              "last-update": update_time,
              "tvmaze-id" : tvmaze_Id,
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
    cursor = c.execute("SELECT Id, tvmaze_Id from TVSHOWS ;")
    all_records = cursor.fetchall()
    if len(all_records):
      cursor = c.execute("SELECT Id from TVSHOWS where tvmaze_Id=?;",(TVmazeId,))
      show_exist = cursor.fetchall()
      if len(show_exist): # show already exist in database, return Id of the show
          con.commit()
          con.close()
          return True,show_exist[0][0]
      else:# show not exist in database, return the max key in database
        cursor = c.execute("SELECT max( Id ) FROM TVSHOWS;")
        ID = cursor.fetchone()[0]
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
      cursor = c.execute("SELECT Id from TVSHOWS where Id=?;",(pre,))
      show_exist = cursor.fetchall()
      if len(show_exist):
          con.commit()
          con.close()
          return pre
      else:
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
        last_update    TEXT,
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
    con.commit()
    con.close()
    app.run(host = host,
        port = port_number,  
        debug = True) 