import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
from firebase_admin import  auth
from firebase_admin import  storage
#from google.cloud import storage
from google.cloud.firestore_v1 import Increment
import sys,traceback
from datetime import datetime
from google.cloud.firestore_v1 import ArrayUnion
import json
import sys,traceback
import qrcode
import io
from flask import jsonify, abort
from functools import wraps





from helpers.HelperFunctions import mstoragebucket,CreateVirtualRoomNewFormat,DeleteVirtualRoomNewFormat,UpdateVirtualRoomNewFormat,firebase_auth_required,json_abort
from google.cloud.firestore_v1 import transactional
from helpers.HelperFunctions import LogicException






#(transaction,db,virtualroomdata,olddata,newdata,virtualroomname,sessionterm,channelid,actiontype,entitytype,entityid)

@firebase_auth_required
def VirtualRoomActionRequestNewFormat(request, decoded_token = None):

    
    data = request.json.get('data')
    print ("new VirtualRoomActionRequest called with data " )
    print(data)
    if 'virtualroomdata' not in data:
        json_abort(400, message="Missing virtualroomdata")
    if 'olddata' not in data:
        json_abort(400, message="Missing olddata")
    if 'newdata' not in data:
        json_abort(400, message="Missing newdata")
    if 'virtualroomname' not in data:
        json_abort(400, message="Missing virtualroomname")
    
    
    if 'actiontype' not in data:
        json_abort(400, message="Missing actiontype")
    
    
    
    if 'entitytype' not in data:
        json_abort(400, message="entitytype")
    if 'entityid' not in data:
        json_abort(400, message="Missing entityid")

    
    entitytype = data['entitytype']
    
    entityid = data['entityid']
    virtualroomdata=  data['virtualroomdata']
    olddata=  data['olddata']
    newdata=  data['newdata']

    virtualroomname=  data['virtualroomname']
    actiontype=  data['actiontype']
    if actiontype !="add" and actiontype !="update" and actiontype !="remove" :
        json_abort(400, message="Not correct value of action type")

    if actiontype =="add":
        if virtualroomdata ==None:
            json_abort(400, message="virtual room data cannot be None")
    elif actiontype =="update":
        if olddata ==None or newdata ==None or virtualroomname ==None  :
            json_abort(400, message="any of virtualroomname ,olddata,newdata cannot be None")
    elif actiontype =="remove":
        if  virtualroomname ==None  :
            json_abort(400, message="any of virtualroomname,sessionterm cannot be None")


    






    try:
        print(' in function call' )
        
        transaction = db.transaction()
        		
        #AddNewVehicle(db,vehdata,entitytype,entityid,entityreg,transaction)

        id1,error=VirtualRoomActionTransactional(transaction,db,virtualroomdata,olddata,newdata,virtualroomname,actiontype,entitytype,entityid)
        data=None
        if actiontype == "add" or actiontype == "update":
            doc=db.collection(entitytype).document(entityid).collection("VIRTUALROOMS").document(id1).get()
            data={}
            if doc.exists:
                data=doc.to_dict()
            else:
                json_abort(400, message="Unknown error")
            
       
        return jsonify({'data': {'id': id1 ,'mdata': data}})

    except LogicException as e:
        s =str(e)
        return jsonify({'data': {'id': None,error:s}})

    except Exception as e:
            print (e )
            print(traceback.format_exc())
            json_abort(400, message= traceback.format_exc())

@transactional
def VirtualRoomActionTransactional(transaction,db,virtualroomdata,olddata,newdata,virtualroomname,actiontype,entitytype,entityid):
    id =virtualroomname
    error=None
    if actiontype =="add":
        id,error =CreateVirtualRoomNewFormat(transaction,db,virtualroomdata,entitytype,entityid)
    elif actiontype == "update":
        id,error =UpdateVirtualRoomNewFormat(transaction,db,virtualroomname,olddata,newdata,entitytype,entityid)
    elif actiontype == "remove":
        doc12 = db.collection(entitytype).document(entityid).collection("VIRTUALROOMS").document(virtualroomname).get(transaction=transaction)
        if doc12.exists:
            virtualroomdata =doc12.to_dict()
            id,error = DeleteVirtualRoomNewFormat(transaction,db,virtualroomdata,entitytype,entityid)
        else:
            error= None
    if error is not None:
        raise LogicException(error)
    return id,error
	
cred = credentials.ApplicationDefault()
#cred = credentials.Certificate('D:/Brindavan_Complex_SchemaBackend/pythonCode/Key/umerflutter-firebase-adminsdk-3qw24-645db10aa5.json')
app = firebase_admin.initialize_app(cred,{
    'storageBucket': mstoragebucket
})


db = firestore.client()
bucket = storage.bucket()
#['COMPLEXES', 'IeWv53ecu7m1di6iMnfl', 'UNITS', 'bld5']
#unittrigger(db,'IeWv53ecu7m1di6iMnfl','COMPLEXES','UNIT1')
