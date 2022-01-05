import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
from firebase_admin import  auth
from firebase_admin import  storage
#from google.cloud import storage
from google.cloud.firestore_v1 import Increment
import sys,traceback
from datetime import datetime,timedelta
import time
from time import mktime
from google.cloud.firestore_v1 import ArrayUnion
import json
import sys,traceback
import qrcode
import io
from flask import jsonify, abort
from functools import wraps
from google.cloud import firestore_v1 as firestore1




from helpers.HelperFunctions import mstoragebucket,OfferingWeeklyScheduleRequest,OfferingModelGroupRequest,firebase_auth_required,json_abort,MakeEntityDefault,getAppointmentCounterValue
from google.cloud.firestore_v1 import transactional
from helpers.HelperFunctions import LogicException






#(transaction,db,virtualroomdata,olddata,newdata,virtualroomname,sessionterm,channelid,actiontype,entitytype,entityid)
#field path - https://firebase.google.com/docs/firestore/quotas#limits
@firebase_auth_required
def GenericQueryActionRequest(request, decoded_token = None):

    
    hdata = request.json.get('data')
    print ("new GenericQueryActionRequest called with data " )
    print(hdata)
    if 'qtype' not in hdata:
        json_abort(400, message="Missing docdata")
    qtype=hdata['qtype']
    if 'entitytype' not in hdata:
        json_abort(400, message="entitytype")
    if 'entityid' not in hdata:
        json_abort(400, message="Missing entityid")

    
    entitytype = hdata['entitytype']
    
    entityid = hdata['entityid']

    try:
        if qtype=='GRADEKINDLISTFROMTEACHEROFFERING':
            if 'grade' not in hdata:
                json_abort(400, message="Missing grade")
            grade =hdata['grade']
            docs =db.collection(entitytype).document(entityid).collection("TEACHEROFFERINGASSIGNMENT").where('grade','==',grade).select([]).stream()
            data = []
            for h in docs:
                data.append(h.id)
            print(data)
            return jsonify({'data':{'l': data,"rt":"l","error":None}})
        if qtype=='GRADEKINDLISTFROMTEACHEROFFERINGJUSTOFFERINGGRP':
            if 'grade' not in hdata:
                json_abort(400, message="Missing grade")
            grade =hdata['grade']
            docs =db.collection(entitytype).document(entityid).collection("TEACHEROFFERINGASSIGNMENT").where('grade','==',grade).select(['ofrgid']).stream()
            data = []
            for h in docs:
                if h.exists:
                    mdata= h.to_dict()
                    if 'ofrgid' in mdata:
                        data.append(mdata['ofrgid'])
                
                
            print(data)
            return jsonify({'data':{'l': data,"rt":"l","error":None}})


        if qtype=='MEDICALTESTNAMES':

            
            docs =db.collection(entitytype).document(entityid).collection("DTESTMODEL").select(["testname"]).stream()
            data = []
            for h in docs:
                myda={}
                myda["id"]=h.id
                myd =h.to_dict()
                myda['name']=myd['testname']
                data.append(myda)
            print(data)
            return jsonify({'data':{'lm': data,"rt":"lm","error":None}})


        if qtype=='GRADEKINDLIST':
            if 'grade' not in hdata:
                json_abort(400, message="Missing grade")
            grade =hdata['grade']
            docs =db.collection(entitytype).document(entityid).collection("OFFERINGMODEL").where('grade','==',grade).select([]).stream()
            data = []
            for h in docs:
                data.append(h.id)
            print(data)
            return jsonify({'data':{'l': data,"rt":"l","error":None}})

        elif qtype=='GRADEVRLIST':
            if 'grade' not in hdata:
                json_abort(400, message="Missing grade")
            grade =hdata['grade']
            docs =db.collection(entitytype).document(entityid).collection("VIRTUALROOMS").where('grade','==',grade).select([]).stream()
            data = []
            for h in docs:
                data.append(h.id)
            print(data)
            return jsonify({'data':{'l': data,"rt":"l","error":None}})

        elif qtype=='staffcategory':
            if 'staffcategory' not in hdata:
                json_abort(400, message="Missing staffcategory")
            staffcategory =hdata['staffcategory']
            docs =db.collection(entitytype).document(entityid).collection("STAFF").where('category','==',staffcategory).select(['name']).stream()
            data = []
            for h in docs:
                data.append({"id":h.id,"display":h.to_dict()['name']})
            print(data)

            return jsonify({'data':{'lm': data,"rt":"lm","error":None}})
        elif qtype=='tripvendor':
            if 'service' not in hdata:
                json_abort(400, message="Missing service")
            service =hdata['service']
            docs =db.collection(entitytype).document(entityid).collection("STAFF").where('services','array_contains',service).select(['name']).stream()
            data = []
            for h in docs:
                data.append({"value":h.id,"name":h.to_dict()['name']})
            print(data)

            return jsonify({'data':{'lm': data,"rt":"lm","error":None}})
        elif qtype=='tripstaff':
            if 'staffcategory' not in hdata:
                json_abort(400, message="Missing staffcategory")
            staffcategory =hdata['staffcategory']
            docs =db.collection(entitytype).document(entityid).collection("STAFF").where('category','==',staffcategory).select(['name']).stream()
            data = []
            for h in docs:
                data.append({"value":h.id,"name":h.to_dict()['name']})
            print(data)

            return jsonify({'data':{'lm': data,"rt":"lm","error":None}})


        elif qtype=='appointmentstaff':
            if 'staffcategory' not in hdata:
                json_abort(400, message="Missing staffcategory")
            staffcategory =hdata['staffcategory']
            docs =db.collection(entitytype).document(entityid).collection("STAFF").where('category','==',staffcategory).select(['name','photo1','educationalqualification','basicbio']).stream()
            data = []
            for h in docs:
                mk=h.to_dict()
                mk['staffid']=h.id
                data.append(mk)
            print(data)

            return jsonify({'data':{'lm': data,"rt":"lm","error":None}})



        elif qtype=='allstaff':
            
            docs =db.collection(entitytype).document(entityid).collection("STAFF").select(['name']).stream()
            data = []
            for h in docs:
                data.append({"id":h.id,"display":h.to_dict()['name']})
            print(data)

            return jsonify({'data':{'lm': data,"rt":"lm","error":None}})
        elif qtype=='INDEPENDENTOFRFORVR':
            if 'virtualroomname' not in hdata:
                json_abort(400, message="Missing virtualroomname")

            virtualroomname =hdata['virtualroomname']
            #sessionterm =hdata['sessionterm']
            docs =db.collection(entitytype).document(entityid).collection('TEACHEROFFERINGASSIGNMENT').where(u'vrlist', u'array_contains', virtualroomname).select([]).stream()
            data=[]
            for h in docs:
                ab= h.id.split('@')
                lenab =len(ab)
                isindependent=ab[4]
                print(h.id)
                if(isindependent=='Y'):
                    data.append(h.id)
            return jsonify({'data':{'l': data,"rt":"l","error":None}})

        elif qtype=='STUFORVR':
            if 'virtualroomname' not in hdata:
                json_abort(400, message="Missing virtualroomname")
            if 'sessionterm' not in hdata:
                json_abort(400, message="Missing sessionterm")
            virtualroomname =hdata['virtualroomname']
            sessionterm =hdata['sessionterm']
            doc =db.collection(entitytype).document(entityid).collection('SESSIONTERM').document(sessionterm).collection("STUDATAVR").document(virtualroomname).get()
            data = []
            if doc.exists:
                m = doc.to_dict()
                data=m['listofregisterid']
            print(data)

            return jsonify({'data':{'lm': data,"rt":"lm","error":None}})
        elif qtype=='STUFOROFR':
            if 'offeringname' not in hdata:
                json_abort(400, message="Missing offeringname")
            if 'sessionterm' not in hdata:
                json_abort(400, message="Missing sessionterm")
            offeringname =hdata['offeringname']
            sessionterm =hdata['sessionterm']
            doc =db.collection(entitytype).document(entityid).collection('SESSIONTERM').document(sessionterm).collection("STUDATAOFR").document(offeringname).get()
            data = []
            if doc.exists:
                m = doc.to_dict()
                data=m['listofregisterid']

            print(data)
            return jsonify({'data':{'lm': data,"rt":"lm","error":None}})
        elif qtype=='ATTENDENCEVRSTAFF':
            if 'virtualroomname' not in hdata:
                json_abort(400, message="Missing virtualroomname")
            if 'sessionterm' not in hdata:
                json_abort(400, message="Missing sessionterm")
            if 'mdate' not in hdata:
                json_abort(400, message="Missing mdate")
            if 'kind' not in hdata:
                json_abort(400, message="Missing kind")

            virtualroomname =hdata['virtualroomname']
            sessionterm =hdata['sessionterm']
            mdate =hdata['mdate']
            kind =hdata['kind']
            dockey =str(int(mdate))+"@" +kind
            fieldname ="adata"+".f_"+kind
            doc =db.collection(entitytype).document(entityid).collection("SESSIONTERM").document(sessionterm).collection("VIRTUALROOMS").document(virtualroomname).collection("ATTENDENCE").document(dockey).get([fieldname])
            data = {}
            if doc.exists:
                m = doc.to_dict()
                if m:
                    data=m['adata']["f_"+kind]
            if data:
                data['sti'] = combineIDDataWithInteractionData(data['sti'],sessionterm,virtualroomname,None,entitytype,entityid)
            else:
                data['sti'] = combineIDDataWithInteractionData([],sessionterm,virtualroomname,None,entitytype,entityid)
            print(data)

            return jsonify({'data':{'m': data,"rt":"m","error":None}})
        elif qtype=='PROGRESSVRSTAFF':
            if 'virtualroomname' not in hdata:
                json_abort(400, message="Missing virtualroomname")
            if 'sessionterm' not in hdata:
                json_abort(400, message="Missing sessionterm")
            if 'kind' not in hdata:
                json_abort(400, message="Missing kind")
            virtualroomname =hdata['virtualroomname']
            sessionterm =hdata['sessionterm']
            kind =hdata['kind']
            dockey =kind
            fieldname ="adata"+ "."+"f_"+kind
            doc =db.collection(entitytype).document(entityid).collection("SESSIONTERM").document(sessionterm).collection("VIRTUALROOMS").document(virtualroomname).collection("PROGRESS").document(dockey).get([fieldname])
            data = {}
            if doc.exists:
                m = doc.to_dict()
                if m:
                    data=m['adata']["f_"+kind]
            if data:
                data['sti'] = combineIDDataWithInteractionData(data['sti'],sessionterm,virtualroomname,None,entitytype,entityid)
            else:
                data['sti'] = combineIDDataWithInteractionData([],sessionterm,virtualroomname,None,entitytype,entityid)
            print(data)

            return jsonify({'data':{'m': data,"rt":"m","error":None}})
        elif qtype=='EVENTVRSTAFF':
            if 'virtualroomname' not in hdata:
                json_abort(400, message="Missing virtualroomname")
            if 'sessionterm' not in hdata:
                json_abort(400, message="Missing sessionterm")
            if 'mdate' not in hdata:
                json_abort(400, message="Missing mdate")
            if 'kind' not in hdata:
                json_abort(400, message="Missing kind")

            virtualroomname =hdata['virtualroomname']
            sessionterm =hdata['sessionterm']
            mdate =hdata['mdate']
            kind =hdata['kind']
            updatedatestr =str(int(mdate))
            dockey =updatedatestr
            fieldname ="adata"+"."+ "f_"+str(mdate)+"."+"f_"+kind
            doc =db.collection(entitytype).document(entityid).collection("SESSIONTERM").document(sessionterm).collection("VIRTUALROOMS").document(virtualroomname).collection("EVENT").document(dockey).get([fieldname])
            data = {}
            if doc.exists:
                m = doc.to_dict()
                if m:
                    data=m['adata']["f_"+updatedatestr]["f_"+kind]
            print(data)

            return jsonify({'data':{'m': data,"rt":"m","error":None}})
        elif qtype=='ATTENDENCEMULTISTAFF':
            if 'sessionterm' not in hdata:
                json_abort(400, message="Missing sessionterm")
            if 'mdate' not in hdata:
                json_abort(400, message="Missing mdate")
            if 'kind' not in hdata:
                json_abort(400, message="Missing kind")
            if 'offeringname' not in hdata:
                json_abort(400, message="Missing offeringname")
            offeringname =hdata['offeringname']
            
            sessionterm =hdata['sessionterm']
            mdate =hdata['mdate']
            kind =hdata['kind']
            dockey =str(mdate)+"@"+kind+"@att"
            fieldname ="adata"+"."+"f_"+kind
            doc =db.collection(entitytype).document(entityid).collection("SESSIONTERM").document(sessionterm).collection("MULTI").document(dockey).get([fieldname])
            data = {}
            if doc.exists:
                m = doc.to_dict()
                if m:
                    data=m['adata']["f_"+kind]            

            if data:
                data['sti'] = combineIDDataWithInteractionData(data['sti'],sessionterm,None,offeringname,entitytype,entityid)
            else:
                data['sti'] = combineIDDataWithInteractionData([],sessionterm,None,offeringname,entitytype,entityid)
            print(data)

            return jsonify({'data':{'m': data,"rt":"m","error":None}})
        elif qtype=='PROGRESSMULTISTAFF':
            if 'sessionterm' not in hdata:
                json_abort(400, message="Missing sessionterm")
            if 'kind' not in hdata:
                json_abort(400, message="Missing kind")
            if 'offeringname' not in hdata:
                json_abort(400, message="Missing offeringname")
            offeringname =hdata['offeringname']
            
            sessionterm =hdata['sessionterm']
            kind =hdata["kind"]
            dockey =kind + "@pro"
            fieldname ="adata"+"."+"f_"+kind
            doc =db.collection(entitytype).document(entityid).collection("SESSIONTERM").document(sessionterm).collection("MULTI").document(dockey).get([fieldname])
            data = {}
            if doc.exists:
                m = doc.to_dict()
                if m:
                    data=m['adata']["f_"+kind]
            #combineIDDataWithInteractionData(interactiondata,sessionterm,virtualroomname,offeringname,entitytype,entityid)
            #data['sti'] = combineIDDataWithInteractionData(data['sti'],sessionterm,None,offeringname,entitytype,entityid)
            if data:
                data['sti'] = combineIDDataWithInteractionData(data['sti'],sessionterm,None,offeringname,entitytype,entityid)
            else:
                data['sti'] = combineIDDataWithInteractionData([],sessionterm,None,offeringname,entitytype,entityid)

            return jsonify({'data':{'m': data,"rt":"m","error":None}})
            print(data)

        elif qtype=='EVENTMULTISTAFF':
            if 'sessionterm' not in hdata:
                json_abort(400, message="Missing sessionterm")
            if 'kind' not in hdata:
                json_abort(400, message="Missing kind")
            if 'mdate' not in hdata:
                json_abort(400, message="Missing mdate")

            sessionterm =hdata['sessionterm']
            kind =hdata["kind"]
            mdate =hdata['mdate']
            updatedatestr =str(int(mdate))
            dockey =updatedatestr+"@"+kind+"@evt"
            fieldname ="adata"+"."+ "f_"+updatedatestr+"."+"f_"+kind
            doc =db.collection(entitytype).document(entityid).collection("SESSIONTERM").document(sessionterm).collection("MULTI").document(dockey).get([fieldname])
            data = {}
            if doc.exists:
                m = doc.to_dict()
                if m:
                    data=m['adata']["f_"+updatedatestr]["f_"+kind]
            return jsonify({'data':{'m': data,"rt":"m","error":None}})
        elif qtype=='IDCARDATTENDENCE':
            if 'sessionterm' not in hdata:
                json_abort(400, message="Missing sessionterm")
            if 'startdate' not in hdata:
                json_abort(400, message="Missing startdate")
            if 'enddate' not in hdata:
                json_abort(400, message="Missing endate")
            if 'id' not in hdata:
                json_abort(400, message="Missing id")
            sessionterm =hdata['sessionterm']
            startdate =hdata['startdate']
            enddate =hdata['enddate']
            id =hdata['id']
            startdatetime = datetime.fromtimestamp(startdate)
            enddatetime=datetime.fromtimestamp(enddate)
            myfieldarray=[]
            while startdatetime <=enddatetime:
                mtime=mktime(startdatetime.timetuple())
                myfieldarray.append("adata.f_"+str(int(mtime)))
                startdatetime=startdatetime+ timedelta(days=1)
            dockey =id+"@att"
            fieldname =myfieldarray
            doc =db.collection(entitytype).document(entityid).collection("SESSIONTERM").document(sessionterm).collection("STUDATA").document(dockey).get(fieldname)
            data = {}
            if doc.exists:
                m = doc.to_dict()
                if m:
                    data=m['adata']
            return jsonify({'data':{'lm': parseIDCARDattendenceUpdated(data),"rt":"lm","error":None}})
            print(data)

        elif qtype=='IDCARDPROGRESS':
            if 'sessionterm' not in hdata:
                json_abort(400, message="Missing sessionterm")
            if 'id' not in hdata:
                json_abort(400, message="Missing id")
            sessionterm =hdata['sessionterm']
            id =hdata['id']
            dockey =id+"@pro"
            fieldname =["adata"]
            doc =db.collection(entitytype).document(entityid).collection("SESSIONTERM").document(sessionterm).collection("STUDATA").document(dockey).get(fieldname)
            data = {}
            if doc.exists:
                m = doc.to_dict()
                if m:
                    data=m['adata']
            return jsonify({'data':{'lm': parseIDCARDprogressUpdated(data),"rt":"lm","error":None}})
        elif qtype=='IDCARDEVENT':
            if 'sessionterm' not in hdata:
                json_abort(400, message="Missing sessionterm")
            if 'startdate' not in hdata:
                json_abort(400, message="Missing startdate")
            if 'enddate' not in hdata:
                json_abort(400, message="Missing endate")
            if 'virtualroomname' not in hdata:
                json_abort(400, message="Missing virtualroomname")
            sessionterm =hdata['sessionterm']
            startdate =hdata['startdate']
            enddate =hdata['enddate']
            virtualroomname =hdata['virtualroomname']
            startdatetime = datetime.fromtimestamp(startdate)
            enddatetime=datetime.fromtimestamp(enddate)
            docpathArray=[]
            while startdatetime <=enddatetime:
                mtime=mktime(startdatetime.timetuple())
                docpathArray.append(db.document(entitytype+'/'+entityid+'/SESSIONTERM/'+sessionterm +'/VIRTUALROOMS/'+virtualroomname+'/EVENT/'+str(int(mtime))))
                startdatetime=startdatetime+ timedelta(days=1)
            #docs =db.collection(entitytype).document(entityid).collection("SESSIONTERM").document(sessionterm).collection("VIRTUALROOMS").document(virtualroomname).collection("EVENT").where(firestore1.field_path.FieldPath.document_id(), "in", docpathArray).stream()
            #docs =db.collection(entitytype).document(entityid).collection("SESSIONTERM").document(sessionterm).collection("VIRTUALROOMS").document(virtualroomname).collection("EVENT").where(firestore1.field_path.FieldPath.document_id(), "in", docpathArray).stream()
            docs=db.get_all(docpathArray)
            data = []
            
            for d in docs:
                m = d.to_dict()
                if m:
                    data.append(m['adata'])

            return jsonify({'data':{'lm': parseIDCARDEVENTUpdated(data),"rt":"lm","error":None}})
        elif qtype=='VRASSIGNMENTLISTNOTINDEPENDENTOFFERINGTEACHER':
            if 'sessionterm' not in hdata:
                json_abort(400, message="Missing sessionterm")
            if 'virtualroomname' not in hdata:
                json_abort(400, message="Missing virtualroomname")
            if 'offeringname' not in hdata:
                json_abort(400, message="Missing offeringlist")
            sessionterm =hdata['sessionterm']
            virtualroomname =hdata['virtualroomname']
            offeringname =hdata['offeringname']
            data = []
            #.where("offering", "==", offeringname).where("virtualroom", "==", virtualroomname)
            if virtualroomname !=None:
                docs=db.collection(entitytype).document(entityid).collection("SESSIONTERM").document(sessionterm).collection("VRASSIGNMENT").where("offering", "==", offeringname).where("virtualroom", "==", virtualroomname).stream()
                for doc in docs:
                    m = doc.to_dict()
                    m['vrid']=doc.id
                    data.append(m)
            return jsonify({'data':{'lm': data,"rt":"lm","error":None}})
        elif qtype=='VRASSIGNMENTLISTINDEPENDENTOFFERINGTEACHER':
            if 'sessionterm' not in hdata:
                json_abort(400, message="Missing sessionterm")
            if 'offeringname' not in hdata:
                json_abort(400, message="Missing offeringlist")
            sessionterm =hdata['sessionterm']
            offeringname =hdata['offeringname']
            data = []
            if offeringname !=None:
                docs = db.collection(entitytype).document(entityid).collection("SESSIONTERM").document(sessionterm).collection("VRASSIGNMENT").where("offering", "==", offeringname).stream()
                for doc in docs:
                    m = doc.to_dict()
                    m['vrid']=doc.id
                    data.append(m)

            return jsonify({'data':{'lm': data,"rt":"lm","error":None}})

        elif qtype=='VRASSIGNMENTLISTSTUDENT':
            if 'sessionterm' not in hdata:
                json_abort(400, message="Missing sessionterm")
            if 'virtualroomname' not in hdata:
                json_abort(400, message="Missing virtualroomname")
            if 'offeringlist' not in hdata:
                json_abort(400, message="Missing offeringlist")
            sessionterm =hdata['sessionterm']
            virtualroomname =hdata['virtualroomname']
            offeringlist =hdata['offeringlist']
            data = []
            if virtualroomname !=None:
                docs = db.collection(entitytype).document(entityid).collection("SESSIONTERM").document(sessionterm).collection("VRASSIGNMENT").where("virtualroom", "==", virtualroomname).stream()
                for doc in docs:
                    m = doc.to_dict()
                    m['vrid']=doc.id
                    data.append(m)
            if offeringlist !=None:
                docs = db.collection(entitytype).document(entityid).collection("SESSIONTERM").document(sessionterm).collection("VRASSIGNMENT").where("offering", "in", offeringlist).stream()
                for doc in docs:
                    m = doc.to_dict()
                    m['vrid']=doc.id
                    data.append(m)
            return jsonify({'data':{'lm': data,"rt":"lm","error":None}})

        elif qtype=='ANSWERLISTSINGLESTUDENT':
            if 'sessionterm' not in hdata:
                json_abort(400, message="Missing sessionterm")
            if 'vrasgid' not in hdata:
                json_abort(400, message="Missing vrasgid")
            if 'id' not in hdata:
                json_abort(400, message="Missing idcardnum")
            sessionterm =hdata['sessionterm']
            id =hdata['id']
            vrasgid =hdata['vrasgid']
            data = []
            docpath =entitytype+'/'+entityid+'/SESSIONTERM/'+sessionterm+'/VRASSIGNMENTSCORE/'+vrasgid
            docs=db.collection(entitytype).document(entityid).collection("SESSIONTERM").document(sessionterm).collection("VRASSIGNMENTSCORE").where(firestore1.field_path.FieldPath.document_id(), "==", db.document(docpath)).select(["sc_"+id]).stream()
            mdata={}
            for doc in docs:
                if doc.exists:
                    mdata = doc.to_dict()
                    if "sc_"+id in mdata:
                        data.append(mdata["sc_"+id])
                else:
                    data.append({'studentid':id,'scoredlevel':-1})
            return jsonify({'data':{'lm': data,"rt":"lm","error":None}})
        elif qtype=='ANSWERLISTALLSTUDENTOFR':
            if 'sessionterm' not in hdata:
                json_abort(400, message="Missing sessionterm")
            if 'vrasgid' not in hdata:
                json_abort(400, message="Missing vrasgid")
            if 'offeringname' not in hdata:
                json_abort(400, message="Missing offeringname")
            sessionterm =hdata['sessionterm']
            offeringname=hdata['offeringname']
            vrasgid =hdata['vrasgid']
            listofregisterid=[]
            doc =db.collection(entitytype).document(entityid).collection('SESSIONTERM').document(sessionterm).collection("STUDATAOFR").document(offeringname).get()
            data = []
            if doc.exists:
                m = doc.to_dict()
                listofregisterid=m['listofregisterid']


            mdata = {}
            
            doc=db.collection(entitytype).document(entityid).collection("SESSIONTERM").document(sessionterm).collection("VRASSIGNMENTSCORE").document(vrasgid).get()

            if doc.exists:
                mdata = doc.to_dict()

            data=[]
            for mstu in listofregisterid:
                id=mstu['id']
                key ="sc"+"_"+id
                if key in mdata:
                    data.append(mdata[key])
                else:
                    m1={}
                    m1['studentid']=id
                    m1['stuname']=mstu['name']
                    m1['scoredlevel']=0
                    data.append(m1)
            return jsonify({'data':{'lm': data,"rt":"lm","error":None}})

        elif qtype=='ANSWERLISTALLSTUDENTVR':
            if 'sessionterm' not in hdata:
                json_abort(400, message="Missing sessionterm")
            if 'vrasgid' not in hdata:
                json_abort(400, message="Missing vrasgid")
            if 'virtualroomname' not in hdata:
                json_abort(400, message="Missing virtualroomname")

            sessionterm =hdata['sessionterm']
            virtualroomname=hdata['virtualroomname']
            vrasgid =hdata['vrasgid']
            listofregisterid=[]
            doc =db.collection(entitytype).document(entityid).collection('SESSIONTERM').document(sessionterm).collection("STUDATAVR").document(virtualroomname).get()
            data = []
            if doc.exists:
                m = doc.to_dict()
                listofregisterid=m['listofregisterid']
            mdata = {}
            
            doc=db.collection(entitytype).document(entityid).collection("SESSIONTERM").document(sessionterm).collection("VRASSIGNMENTSCORE").document(vrasgid).get()
            if doc.exists:
                mdata = doc.to_dict()

            data=[]
            for mstu in listofregisterid:
                id=mstu['id']
                key ="sc"+"_"+id
                if key in mdata:
                    data.append(mdata[key])
                else:
                    m1={}
                    m1['studentid']=id
                    m1['stuname']=mstu['name']
                    m1['scoredlevel']=0
                    data.append(m1)


            return jsonify({'data':{'lm': data,"rt":"lm","error":None}})
        elif qtype=='ENTITYSERVICETYPE':
            if 'serviceidlist' not in hdata:
                json_abort(400, message="Missing serviceid")
            serviceidlist = hdata['serviceidlist']
            category=hdata['category']
            docpathArray=[]
            sdata = []
            if serviceidlist:
                for serviceid in serviceidlist:
                    docpathArray.append(db.document('SERVICEPROVIDERINFO'+'/'+serviceid))
                #only 10 members are allowed in one go
                myMax10sizedArrayList =getArraySplitForMaxSize10(docpathArray)    
                for docpathArray_sizedMax10 in myMax10sizedArrayList:
                    docs =db.collection('SERVICEPROVIDERINFO').where(firestore1.field_path.FieldPath.document_id(), "in", docpathArray_sizedMax10).select(['servicename','servicetype']).stream()
                    
                    for d in docs:
                        m = d.to_dict()
                        if m['servicetype'] is not None and category in m['servicetype']:
                            sdata.append({"id":d.id,"name":m['servicename']})
            return jsonify({'data':{'lm': sdata,"rt":"lm","error":None}})

        elif qtype=='ENTITYNAMES':
            if 'serviceidlist' not in hdata:
                json_abort(400, message="Missing serviceid")
            if 'complexidlist' not in hdata:
                json_abort(400, message="Missing serviceid")
            serviceidlist = hdata['serviceidlist']
            complexidlist = hdata['complexidlist']
            docpathArray=[]
            sdata = []
            if serviceidlist:
                for serviceid in serviceidlist:
                    docpathArray.append(db.document('SERVICEPROVIDERINFO'+'/'+serviceid))
                #only 10 members are allowed in one go
                myMax10sizedArrayList =getArraySplitForMaxSize10(docpathArray)    
                for docpathArray_sizedMax10 in myMax10sizedArrayList:
                    docs =db.collection('SERVICEPROVIDERINFO').where(firestore1.field_path.FieldPath.document_id(), "in", docpathArray_sizedMax10).select(['servicename']).stream()
                    
                    for d in docs:
                        m = d.to_dict()
                        sdata.append({"id":d.id,"name":m['servicename']})
            cdata = []
            if complexidlist:
                for complexid in complexidlist:
                    docpathArray.append(db.document('COMPLEXES'+'/'+complexid))

                myMax10sizedArrayList =getArraySplitForMaxSize10(docpathArray)    
                for docpathArray_sizedMax10 in myMax10sizedArrayList:
                    docs =db.collection('COMPLEXES').where(firestore1.field_path.FieldPath.document_id(), "in", docpathArray_sizedMax10).select(['complexName']).stream()

                    for d in docs:
                        m = d.to_dict()
                        cdata.append({"id":d.id,"name":m['complexName']})

            maindata={}
            maindata["c"]=cdata
            maindata["s"]=sdata
            return jsonify({'data':{'m': maindata,"rt":"m","error":None}})
        elif qtype=='MAKEENTITYDEFAULT':
            if 'userid' not in hdata:
                json_abort(400, message="Missing userid")

            entitytype = hdata['entitytype']
            entityid = hdata['entityid']
            userid =hdata['userid']
            isstaff =hdata['isstaff']
            transaction = db.transaction()
            maindata =MakeEntityDefaultTrans(transaction,db,entitytype,entityid,userid,isstaff)
            md = db.collection('USERINFO').document(userid).get()
            
            return jsonify({'data':{'m': md.to_dict(),"rt":"m","error":None}})


        elif qtype=='GETAPPOINTMENTCOUNTERVALUE':
            if 'ownerId' not in hdata:
                json_abort(400, message="Missing userid")
            if 'date' not in hdata:
                json_abort(400, message="Missing date")
            if 'period' not in hdata:
                json_abort(400, message="Missing period")

            entitytype = hdata['entitytype']
            entityid = hdata['entityid']
            ownerId =hdata['ownerId']
            mdate =hdata['date']
            period =hdata['period']
            transaction = db.transaction()
            maindata =getAppointmentCounterValuetransactional(transaction,db,entitytype,entityid,ownerId,mdate,period)
            return jsonify({'data':{'lm': maindata,"rt":"lm","error":None}})



        else:
            print ("unknown parameter type " +qtype)
            return jsonify({'data':{ "error":"Query type not defined"}})



    except LogicException as e:
        s =str(e)
        return jsonify({'data':{'id': None,'error':s}})

    except Exception as e:
            print (e )
            print(traceback.format_exc())
            json_abort(400, message= traceback.format_exc())

def getArraySplitForMaxSize10(myarray):
    mynewarray =[]
    mynewchild=[]

    if len(myarray) <10:
        mynewarray.append(myarray)
    else:
        for k in   myarray:
            if len(mynewchild) ==10:
                mynewarray.append(mynewchild)
                mynewchild=[]
            mynewchild.append(k)
        
    if len(mynewchild)  >0:
        mynewarray.append(mynewchild)
    return mynewarray


@transactional
def getAppointmentCounterValuetransactional(transaction,db,entitytype,entityid,ownerId,mdate,period):
    mydata=getAppointmentCounterValue(transaction,db,entitytype,entityid,ownerId,mdate,period)
    return mydata


@transactional
def MakeEntityDefaultTrans(transaction,db,entitytype,entityid,userid,isstaff):
    id,error = MakeEntityDefault(transaction,db,entitytype,entityid,userid,isstaff,False)
    return {"id":id,"error":error}



def getIDlistForVROfOFR(sessionterm,virtualroomname,offeringname,entitytype,entityid):
    data = {}
    doc={}
    if offeringname ==None:
        doc =db.collection(entitytype).document(entityid).collection('SESSIONTERM').document(sessionterm).collection("STUDATAVR").document(virtualroomname).get()
    else:
        doc =db.collection(entitytype).document(entityid).collection('SESSIONTERM').document(sessionterm).collection("STUDATAOFR").document(offeringname).get()
    if doc.exists:
        m = doc.to_dict()
        data=m['listofregisterid']
    return data
    
    

def combineIDDataWithInteractionData(interactiondata,sessionterm,virtualroomname,offeringname,entitytype,entityid):
    
    idcardlist =getIDlistForVROfOFR(sessionterm,virtualroomname,offeringname,entitytype,entityid)
    mymap ={}
    for s in interactiondata:
        mymap[s['id']]=s
    newdata=[]
    for k in idcardlist:
        myinfo={}
        if k['id'] in mymap:
            myinfo=mymap[k['id']]
        if not myinfo:
            myinfo={"id":k['id'],"val":"C"}
            if offeringname is not None:
                myinfo['vr'] =k['vr']
            else:
                myinfo['rno'] =k['rno']

        myinfo['name']=k['name']
        newdata.append(myinfo)
    return newdata

def parseIDCARDattendence(mydata):
    #mydata={'f_1594008000': {'f_FIRST': 'A', 'f_SCIENCE': 'A'},'f_1594008001': {'f_FIRST': 'A', 'f_SCIENCE': 'A'}}
    returndatalist=[]
    for pkey in mydata.keys():
        returndata={}
        returndata['key']=pkey
        keydata= mydata[pkey]
        firstleveldata=[]
        returndata['value']=firstleveldata
        for lkey in keydata.keys():
            data={}
            data['name']=lkey
            data['value']=keydata[lkey]
            firstleveldata.append(data)
        returndatalist.append(returndata)
    return returndatalist

def parseIDCARDprogress(mydata):
    #mydata={'f_SCIENCE_d_mainterm1_a_Subterm1': {'f_un': '91/100'}, 'f_SCIENCE_d_mainterm1_a_Subterm2': {'f_un': '91/100'}, 'f_HINDI_d_mainterm1_a_Subterm1': {'f_un': '81/100'}, 'f_HINDI_d_mainterm1_a_Subterm2': {'f_un': '81/100'}, 'f_ENGLISH_d_mainterm1_a_Subterm1': {'f_un': '81/100'}, 'f_ENGLISH_d_mainterm1_a_Subterm2': {'f_un': '81/100'}, 'f_MATH_d_mainterm1_a_Subterm1': {'f_un': '81/100'}, 'f_MATH_d_mainterm1_a_Subterm2': {'f_un': '81/100'}}
    returndatalist=[]
    for pkey in mydata.keys():
        returndata={}
        returndata['key']=pkey
        keydata= mydata[pkey]
        firstleveldata=[]
        returndata['value']=firstleveldata
        for lkey in keydata.keys():
            data={}
            data['name']=lkey
            data['value']=keydata[lkey]
            firstleveldata.append(data)
        returndatalist.append(returndata)
    return returndatalist


def parseIDCARDEVENT(mydata):
    #mydata=[{'f_1594008000': {'f_HINDI': {'sti': 'hello from HINDI    1594008000', 'kind': 'HINDI', 'mdate': 1594008000}, 'f_MATH': {'sti': 'hello from MATH    1594008000', 'kind': 'MATH', 'mdate': 1594008000}, 'f_SCIENCE': {'sti': 'hello from SCIENCE    1594008000', 'kind': 'SCIENCE', 'mdate': 1594008000}, 'f_ENGLISH': {'sti': 'hello from ENGLISH    1594008000', 'kind': 'ENGLISH', 'mdate': 1594008000}}}]
    returndatalist=[]
    for mdatelevel in mydata:
        print(type(mdatelevel))
        for pkey in mdatelevel.keys():
            returndata={}
            returndata['key']=pkey
            keydata= mdatelevel[pkey]
            firstleveldata=[]
            returndata['value']=firstleveldata
            for lkey in keydata.keys():
                data={}
                lkeydata=keydata[lkey]
                data['name']=lkeydata['kind']
                data['value']=lkeydata['sti']
                firstleveldata.append(data)
            returndatalist.append(returndata)

    return returndatalist




def parseIDCARDattendenceUpdated(mydata):
    #mydata={'f_1594008000': {'f_FIRST': 'A', 'f_SCIENCE': 'A'},'f_1594008001': {'f_FIRST': 'A', 'f_SCIENCE': 'A'}}
    returndatalist=[]
    for pkey in mydata.keys():
        returndata={}
        #returndata['key']=pkey
        keydata= mydata[pkey]
        firstleveldata=[]
        #returndata['value']=firstleveldata
        returndata[pkey]=firstleveldata
        for lkey in keydata.keys():
            data={}
            #data['name']=lkey
            #data['value']=keydata[lkey]
            data[lkey]=keydata[lkey]
            firstleveldata.append(data)
        returndatalist.append(returndata)
    return returndatalist

def parseIDCARDprogressUpdated(mydata):
    #mydata={'f_SCIENCE_d_mainterm1_a_Subterm1': {'f_un': '91/100'}, 'f_SCIENCE_d_mainterm1_a_Subterm2': {'f_un': '91/100'}, 'f_HINDI_d_mainterm1_a_Subterm1': {'f_un': '81/100'}, 'f_HINDI_d_mainterm1_a_Subterm2': {'f_un': '81/100'}, 'f_ENGLISH_d_mainterm1_a_Subterm1': {'f_un': '81/100'}, 'f_ENGLISH_d_mainterm1_a_Subterm2': {'f_un': '81/100'}, 'f_MATH_d_mainterm1_a_Subterm1': {'f_un': '81/100'}, 'f_MATH_d_mainterm1_a_Subterm2': {'f_un': '81/100'}}
    returndatalist=[]
    for pkey in mydata.keys():
        returndata={}
        #returndata['key']=pkey
        keydata= mydata[pkey]
        firstleveldata=[]
        #returndata['value']=firstleveldata
        returndata[pkey]=firstleveldata        
        for lkey in keydata.keys():
            data={}
            #data['name']=lkey
            #data['value']=keydata[lkey]
            data[lkey]=keydata[lkey]
            firstleveldata.append(data)

        returndatalist.append(returndata)
    return returndatalist


def parseIDCARDEVENTUpdated(mydata):
    #mydata=[{'f_1594008000': {'f_HINDI': {'sti': 'hello from HINDI    1594008000', 'kind': 'HINDI', 'mdate': 1594008000}, 'f_MATH': {'sti': 'hello from MATH    1594008000', 'kind': 'MATH', 'mdate': 1594008000}, 'f_SCIENCE': {'sti': 'hello from SCIENCE    1594008000', 'kind': 'SCIENCE', 'mdate': 1594008000}, 'f_ENGLISH': {'sti': 'hello from ENGLISH    1594008000', 'kind': 'ENGLISH', 'mdate': 1594008000}}}]
    returndatalist=[]
    for mdatelevel in mydata:
        
        for pkey in mdatelevel.keys():
            returndata={}
            #returndata['key']=pkey
            keydata= mdatelevel[pkey]
            firstleveldata=[]
            #returndata['value']=firstleveldata
            returndata[pkey]=firstleveldata            
            for lkey in keydata.keys():
                data={}
                lkeydata=keydata[lkey]
                #data['name']=lkeydata['kind']
                #data['value']=lkeydata['sti']
                data[lkeydata['kind']]=lkeydata['sti']
                firstleveldata.append(data)
            returndatalist.append(returndata)

    return returndatalist




cred = credentials.ApplicationDefault()
#cred = credentials.Certificate('D:/Brindavan_Complex_SchemaBackend/pythonCode/Key/umerflutter-firebase-adminsdk-3qw24-645db10aa5.json')
app = firebase_admin.initialize_app(cred,{
    'storageBucket': mstoragebucket
})


db = firestore.client()
bucket = storage.bucket()
#['COMPLEXES', 'IeWv53ecu7m1di6iMnfl', 'UNITS', 'bld5']
#unittrigger(db,'IeWv53ecu7m1di6iMnfl','COMPLEXES','UNIT1')
