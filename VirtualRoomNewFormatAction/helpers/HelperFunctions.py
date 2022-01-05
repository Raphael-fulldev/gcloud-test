#https://googleapis.dev/python/firestore/latest/field_path.html -cloud firestore v1
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
from firebase_admin import  auth
from firebase_admin import  storage
#from google.cloud import storage
from google.cloud.firestore_v1 import Increment
import sys,traceback
from datetime import datetime,date
from google.cloud.firestore_v1 import ArrayUnion,ArrayRemove,field_path
from google.cloud import firestore_v1 as firestore1
import json
import sys,traceback
import qrcode
import io
from flask import jsonify, abort
from functools import wraps
from collections import defaultdict
import calendar
from firebase_admin import messaging
#from HelperFunctions_ReadTrans import generateqrcodeandsave,RoleBasedChannelListFromEntity,ProcessUserRecord
#vehicle doesnt require a related entry , so it type is None
# type - m for staff, r for resident ,  h -homehelp , g - guest/visitor,
staffqrcodetype="m"
vehicleqrcodetype = "v"
#mstoragebucket='sampledatabasedevexpert.appspot.com'
mstoragebucket='brindavan-c61b7.appspot.com'
class LogicException(Exception):
    pass

def json_abort(status_code, message):
    data = {
        'error': {
            'code': status_code,
            'message': message
        }
    }
    response = jsonify(data)
    response.status_code = status_code
    abort(response)


def getutctimestampfornow():
    d = datetime.utcnow()
    epoch = datetime(1970,1,1)
    t = (d - epoch).total_seconds()
    return t

def firebase_auth_required(f):
    @wraps(f)
    def wrapper(request):
        print("in firebase auth")
        print(request)
        authorization = request.headers.get('Authorization')
        id_token = None
        if authorization and authorization.startswith('Bearer '):
            id_token = authorization.split('Bearer ')[1]
        else:
            print ("cant find bearer, aborting")
            json_abort(401, message="Invalid authorization")

        try:
            print (id_token)
            decoded_token = auth.verify_id_token(id_token)
        except Exception as e: # ValueError or auth.AuthError
            print ("decode error")
            json_abort(401, message="Invalid authorization")
        return f(request, decoded_token)
    return wrapper
def checkQRCodeForFee(entitytype,entityid):
    return True
#actiontype -pr_add,pr_update,pr_delete,ch_add,ch_delete
def UserRegistrationFeePaymentProcessingAction(transaction,db,actiontype,prid,prnewdata,prolddata,chid,chnewdata,cholddata,sessionterm,idcardnum,entitytype,entityid):
    if actiontype=="pr_add":
        paymentref = db.collection(entitytype).document(entityid).collection("USERFEEPAYMENT").document()
        paymentamount=prnewdata["totalfeeamount"]
        #usersessionreg = db.collection(entitytype).document(entityid).collection("USERSESSIONREGISTRATION").document(idcardnum+"@"+sessionterm)
        paymentdateref = db.collection(entitytype).document(entityid).collection("USERREGISTRATIONPAYMENTDATES").document(idcardnum)
        parmentdatedoc=None
        paymentdatedoct = paymentdateref.get(transaction=transaction)
        updatepaymentdateflag=False
        if paymentdatedoct.exists:
            parmentdatedoc = paymentdatedoct.to_dict()
        else:
            return None,"Payment date information not found, check with administrator"
#        if "startdate" not in parmentdatedoc or "periodstartdate" not in prnewdata or "enddate" not in parmentdatedoc or "periodenddate" not in prnewdata  or parmentdatedoc["startdate"] != prnewdata["periodstartdate"] or parmentdatedoc["enddate"] != prnewdata["periodenddate"]:
#            return None,"Payment date information doesnt match, check with administrator"
#        if "totalamount" not in parmentdatedoc or "totalfeeamount" not in prnewdata or parmentdatedoc["totalamount"] != prnewdata["totalfeeamount"]:
#            prnewdata["scomment"] =["Payment amount doesnt match stored payment date amount"]
#            updatepaymentdateflag=True
        ##start inserting data
        if updatepaymentdateflag:
            transaction.update(paymentdateref,{"totalamount":prnewdata["totalfeeamount"]})
        transaction.set(paymentref,prnewdata)
        return paymentref.id,None      
        #transaction.set(usersessionreg,ArrayUnion([{"id":paymentref.id,"sd":}]))
    elif actiontype=="pr_update":
        if "startdate" in prnewdata  or "enddate"  in prnewdata or "feeplantype" in prnewdata or "feeplanname" in prnewdata:
            return None, "Not Allowed to change feeplantype or FeePlanName for existing record"
        if "totalamount" in prnewdata:
            prnewdata["scomment"] =ArrayUnion("Total amount changed" + datetime.now().strftime("%I:%M%p on %B %d, %Y"))
        paymentref = db.collection(entitytype).document(entityid).collection("USERFEEPAYMENT").document(prid)
        ab =paymentref.get()
        transaction.update(paymentref,prnewdata)
        return prid,None
    elif actiontype=="pr_delete":
        paymentParentref = db.collection(entitytype).document(entityid).collection("USERFEEPAYMENT").document(prid)
        paymentParentinfo =None
        paymentParentdoct =paymentParentref.get(transaction=transaction)
        if paymentParentdoct.exists:
            paymentParentinfo =paymentParentdoct.to_dict()
        else:
            return None,"Payment Parent record id doesnt exist"
        if paymentParentinfo["totalpaymentmade"] is None or paymentParentinfo["totalpaymentmade"]==0:
            transaction.delete(paymentParentref)
        else:
            return None,"Payment has been made,record cannot be deleted"
        return prid,None
    elif actiontype=="ch_add":
        paymentParentref = db.collection(entitytype).document(entityid).collection("USERFEEPAYMENT").document(prid)
        paymentParentinfo =None
        paymentParentdoct =paymentParentref.get(transaction=transaction)
        if paymentParentdoct.exists:
            paymentParentinfo =paymentParentdoct.to_dict()
        else:
            return None,"Payment Parent record id doesnt exist"
        if "closed" in  paymentParentinfo and paymentParentinfo["closed"]:
            return None,"Payment Parent record is in closed state"

        amountPaid =chnewdata["paymentamount"]
        totalamount = paymentParentinfo["totalfeeamount"]
        paymentmade = paymentParentinfo["totalpaymentmade"] + amountPaid
        balancepayment = totalamount - paymentmade
        idcardnum=paymentParentinfo['idcardnum']
        sessionterm=paymentParentinfo['sessionterm']
        if balancepayment < 0:
            return None,"Balance payment is less than Amount Paid, Please correct"
        if balancepayment == 0:
            transaction.update(paymentParentref,{"closed":True, "totalpaymentmade":paymentmade})
            appuserid=None
            processFeePlanSessionRegistration(transaction,db,paymentParentinfo["feeplantype"],None,paymentParentinfo["paymentperiodname"],idcardnum,paymentParentinfo["paymentperiodname"],appuserid,entitytype,None,entityid,True)
            paymentdateref = db.collection(entitytype).document(entityid).collection("USERREGISTRATIONPAYMENTDATES").document(idcardnum)            
            transaction.update(paymentdateref,{"balance":balancepayment})

        else:
            transaction.update(paymentParentref,{"closed":False, "totalpaymentmade":paymentmade})
            paymentdateref = db.collection(entitytype).document(entityid).collection("USERREGISTRATIONPAYMENTDATES").document(idcardnum)            
            transaction.update(paymentdateref,{"balance":balancepayment})

        paymentChildref = db.collection(entitytype).document(entityid).collection("USERFEEPAYMENT").document(prid).collection("PINFO").document()
        transaction.set(paymentChildref,chnewdata)
        return paymentChildref.id,None
    elif actiontype=="ch_update":
        paymentParentref = db.collection(entitytype).document(entityid).collection("USERFEEPAYMENT").document(prid)
        paymentParentinfo =None
        paymentParentdoct =paymentParentref.get(transaction=transaction)
        if paymentParentdoct.exists:
            paymentParentinfo =paymentParentdoct.to_dict()
        else:
            return None,"Payment Parent record id doesnt exist"

        if "closed" in  paymentParentinfo and paymentParentinfo["closed"]:
            return None,"Payment Parent record is in closed state"
        idcardnum=paymentParentinfo['idcardnum']
        sessionterm=paymentParentinfo['sessionterm']

        paymentChildref = db.collection(entitytype).document(entityid).collection("USERFEEPAYMENT").document(prid).collection("PINFO").document(chid)
        paymentChildinfo =None
        paymentChilddoct =paymentChildref.get(transaction=transaction)
        if paymentChilddoct.exists:
            paymentChildinfo =paymentChilddoct.to_dict()
        else:
            return None,"Payment Child record id doesnt exist"
        if 'paymentamount' in chnewdata:
            amountPaid =chnewdata["paymentamount"]
            orignalAmtPaid =paymentChildinfo["paymentamount"]
            totalamount = paymentParentinfo["totalfeeamount"]
            paymentmade = paymentParentinfo["totalpaymentmade"] + amountPaid - orignalAmtPaid
            balancepayment = totalamount - paymentmade
            if balancepayment < 0:
                return None,"Balance payment is less than Amount Paid, Please correct"
            if balancepayment == 0:
                transaction.update(paymentParentref,{"closed":True, "totalpaymentmade":paymentmade})
                appuserid=None
                processFeePlanSessionRegistration(transaction,db,paymentParentinfo["feeplantype"],None,paymentParentinfo["paymentperiodname"],idcardnum,paymentParentinfo["paymentperiodname"],appuserid,entitytype,None,entityid,True)
                paymentdateref = db.collection(entitytype).document(entityid).collection("USERREGISTRATIONPAYMENTDATES").document(idcardnum)            
                transaction.update(paymentdateref,{"balance":balancepayment})

            else:
                transaction.update(paymentParentref,{"closed":False, "totalpaymentmade":paymentmade})
                paymentdateref = db.collection(entitytype).document(entityid).collection("USERREGISTRATIONPAYMENTDATES").document(idcardnum)            
                transaction.update(paymentdateref,{"balance":balancepayment})

        paymentChildref = db.collection(entitytype).document(entityid).collection("USERFEEPAYMENT").document(prid).collection("PINFO").document(chid)
        transaction.update(paymentChildref,chnewdata)
        return paymentChildref.id,None
    else:
        return None, "Not accepted actiontype"
        



def DiffAddSubMatch(li1, li2,field1):
    diffrst=[]
    for s in li1:
        foundmatch=False
        for p in li2:
            if field1 not in s or field1 not in p or s[field1] is None or p[field1] is None:
                continue
            if s[field1] ==p[field1]:
                foundmatch=True
                break
        if not foundmatch:
            diffrst.append(s)
    return diffrst
##Here l11 represent old data, li2 represent new data, and matching is by field field1
## return delval,addval

def DiffAddSub(li1, li2,field1): 
    if li1 is None and li2 is None:
        return None,None
    elif li1 is None and li2 is not None:
        return None,li2
    elif li1 is not None and li2 is  None:
        return li1,None
    else: 
        delval =DiffAddSubMatch(li1, li2,field1)
        addval =DiffAddSubMatch(li2, li1,field1)
        return delval,addval
        

def DiffAddSubSimple(li1, li2): 
    if li1 is None and li2 is None:
        return None,None
    elif li1 is None and li2 is not None:
        return None,li2
    elif li1 is not None and li2 is  None:
        return li1,None
    else: 
        delval =  list(set(li1).intersection(li2))
        addval = list(set(li2).intersection(li1))
        return delval,addval



def  getFeePlanDataForParticularPeriod(db,idcardnum,feeplan,entitytype,entityid,periodname):
    periodnamefeedata={}
    feeplanref = db.collection(entitytype).document(entityid).collection("FEEPLANS").document(feeplan).get()
    feeplandict=None
    if feeplanref.exists:
        feeplandict = feeplanref.to_dict()
    
    if feeplandict is None:
        return None
    
    feedata = feeplandict['feedata']
    if feedata is None:
        return None
    
    periodnamefeedata["discounttype"] =feeplandict["discounttype"]
    periodnamefeedata["paymentperiodtype"] =feeplandict["paymentperiodtype"]
    curdata =None
    for f in feedata:
        if f["paymentperiodname"] ==periodname:
            curdata = f
            break
    
    if curdata is None:
        periodnamefeedata["perioddata"] =None
        return periodnamefeedata
    periodnamefeedata["perioddata"] =curdata
    feeschnamearray =curdata['feeschedulename']
    if feeschnamearray is None:
        periodnamefeedata["feescheduledata"] = None
        return periodnamefeedata
        
    feeitemdata=[]
    docpathArray=[]

    for k in feeschnamearray:
        docpathArray.append(db.document(entitytype+'/'+entityid+'/FEEITEMGRPS/'+k))
    
    docs =db.collection('SERVICEPROVIDERINFO').document(entityid).collection("FEEITEMGRPS").where(firestore1.field_path.FieldPath.document_id(), "in", docpathArray).stream()
    totalammount =0
    for d in docs:
        kd = d.to_dict()
        feeitemdata.append(kd)
        if "data" not in kd:
            continue
        for s in kd["data"]:
            totalammount=totalammount + s["amount"] 

    periodnamefeedata["feescheduledata"]=feeitemdata
    periodnamefeedata["totalamount"] = totalammount


    return  periodnamefeedata       



def getFeePlanData(db,idcardnum,feeplan,entitytype,entityid):
    feeplanref = db.collection(entitytype).document(entityid).collection("FEEPLANS").document(feeplan).get()
    feeplandict=None
    if feeplanref.exists:
        feeplandict = feeplanref.to_dict()
    
    if feeplandict is None:
        return None
    
    feedata = feeplandict['feedata']
    if feedata is None:
        return None
    
    feedata=sorted(feedata, key=lambda x: (x['duedate']))
    feeplandict['feedata']=feedata

    for f in feedata:
        feeschnamearray =f['feeschedulename']
        if feeschnamearray is None:
            return None
        
        feeitemdata=[]
        docpathArray=[]

        for k in feeschnamearray:
            docpathArray.append(db.document(entitytype+'/'+entityid+'/FEEITEMGRPS/'+k))
        
        docs =db.collection('SERVICEPROVIDERINFO').document(entityid).collection("FEEITEMGRPS").where(firestore1.field_path.FieldPath.document_id(), "in", docpathArray).stream()
        
        for d in docs:
            feeitemdata.append(d.to_dict())
        f["feescheduledata"]=feeitemdata

    return  feeplandict       





def getChannelFromVirtualRoom(transaction,db, sessionterm,virtualroomname,entitytype,entityid):
    docpath =entitytype+'/'+entityid+'/SESSIONTERM/'+sessionterm+'/VIRTUALROOMS/'+virtualroomname
    print(docpath)
    doc1 = transaction.get(db.collection('SERVICEPROVIDERINFO').document(entityid).collection("SESSIONTERM").document(sessionterm).collection("VIRTUALROOMS").where(firestore1.field_path.FieldPath.document_id(), "==", db.document(docpath)).select(['channelid']))
    setflag=False
    channelid=None
    for x in doc1:
        if x.exists:
            m = x.to_dict()
            setflag=setflag+1
            if "channelid" in m:
                channelid = m["channelid"]
    return channelid

def getChannelFromOfferingSch(transaction,db, sessionterm,offerschkey,entitytype,entityid):
    docpath =entitytype+'/'+entityid+'/SESSIONTERM/'+sessionterm+'/OFFERINGSCHEDULE/'+offerschkey
    print(docpath)
    doc1 = transaction.get(db.collection('SERVICEPROVIDERINFO').document(entityid).collection("SESSIONTERM").document(sessionterm).collection("OFFERINGSCHEDULE").where(firestore1.field_path.FieldPath.document_id(), "==", db.document(docpath)).select(['channelid']))
    setflag=False
    channelid=None
    for x in doc1:
        if x.exists:
            m = x.to_dict()
            setflag=setflag+1
            if "channelid" in m:
                channelid = m["channelid"]
    return channelid





def returnUserRecordForVR_OfferingSchUpdated(channelid,profileid,isvr,isadd,userdata,idcardnum,sessionterm,virtualroomname,offeringsch):
    if isadd:
        if isvr==True:
            
            userdata[profileid+".channels_vr"]= ArrayUnion([{"channel":channelid,"id":idcardnum,"rights":"r","st":sessionterm,"vr":virtualroomname}])
        else:
            userdata[profileid+".channels_oc"]= ArrayUnion([{"channel":channelid,"id":idcardnum,"rights":"r","st":sessionterm,"ofr":offeringsch}])
    else:
        if isvr==True:
            userdata[profileid+".channels_vr"]= ArrayRemove([{"channel":channelid,"id":idcardnum,"rights":"r","st":sessionterm,"vr":virtualroomname}])
        else:
            userdata[profileid+".channels_oc"]= ArrayRemove([{"channel":channelid,"id":idcardnum,"rights":"r","st":sessionterm,"ofr":offeringsch}])

    return userdata


def returnUserRecordForVR_OfferingSch(channelid,profileid,isvr,isadd,idcardnum,addvraary,remvraary,addofraary,remofraary,sessionterm,virtualroomname,offeringsch):
    if isadd:
        if isvr==True:
            addvraary.append(  {"channel":channelid,"id":idcardnum,"rights":"r","st":sessionterm,"vr":virtualroomname})
        else:
            addofraary.append({"channel":channelid,"id":idcardnum,"rights":"r","st":sessionterm,"ofr":offeringsch})
    else:
        if isvr==True:
            remvraary.append({"channel":channelid,"id":idcardnum,"rights":"r","st":sessionterm,"vr":virtualroomname})
        else:
            remofraary.append({"channel":channelid,"id":idcardnum,"rights":"r","st":sessionterm,"ofr":offeringsch})

    return addvraary,remvraary,addofraary,remofraary


def updateUserProfileForRegistration(transaction,db,appuserid,gaurdian1userid,gaurdian2userid,userrecord):
    if appuserid is not None:
        userref = db.collection("USERS").document(appuserid)
        #if userref.exists:
        transaction.update(userref,userrecord)
    if gaurdian1userid is not None:
        userref = db.collection("USERS").document(gaurdian1userid)
        #if userref.exists:
        transaction.update(userref,userrecord)


    if gaurdian2userid is not None:
        userref = db.collection("USERS").document(gaurdian2userid)
        #if userref.exists:
        transaction.update(userref,userrecord)


def getUserRegistrationData(transaction,db,entitytype,entityid,idcardnum):
    doc1 = db.collection(entitytype).document(entityid).collection("USERREGISTRATION").document(idcardnum).get(transaction=transaction)
    regdata=None
    if doc1.exists:
        regdata =doc1.to_dict()
    else:
        return None,"User registration record doesnt exist"
    return regdata,None

def getUserSessionRegistrationData(transaction,db,entitytype,entityid,key):
    doc1 = db.collection(entitytype).document(entityid).collection("USERSESSIONREGISTRATION").document(key).get(transaction=transaction)
    regdata=None
    if doc1.exists:
        regdata =doc1.to_dict()
    else:
        return None,"User registration record doesnt exist"
    return regdata,None


def processFeePlanSessionRegistration(transaction,db,feeplantype,usersessioninformation,periodname,idcardnum,feeplan,appuserid,entitytype,dateofjoining,entityid,nextperiodflag):
    if feeplantype=='FEEPLAN':
        feeplandata=getFeePlanData(db,idcardnum,feeplan,entitytype,entityid)
        if usersessioninformation is not None:
            usersessioninformation["feeplandata"]=feeplandata
        curdata=None
        reqcounter=None
        feedata = feeplandata['feedata']
        if periodname is not None:
            for counter in range(0,len(feedata),1):
                if feedata[counter]['paymentperiodname']==periodname:
                    curdata= feedata[counter]
                    reqcounter=counter
                    break
            if curdata is None:
                return None, "cur period date not found"
            if nextperiodflag and reqcounter +1 >=len(feedata):
                return feeplandata,{}
            if nextperiodflag:
                curdata= feedata[reqcounter+1]
            
        else:
            curdata=  feedata[0]
        feepaymentdatesrec={}
        feepaymentdatesrec['feeplantype'] = 'FEEPLAN'
        feepaymentdatesrec['feeplan'] = feeplan
        feepaymentdatesrec['appuserid'] = appuserid
        feepaymentdatesrec['startdate'] = curdata['startdate']
        feepaymentdatesrec['enddate'] = curdata['enddate']
        feepaymentdatesrec['paymentperiodname'] = curdata['paymentperiodname']
        feepaymentdatesrec['isfirst'] = True
        feepaymentdatesrec['totalamount'] = curdata['totalamount']
        feepaymentdatesrec['duedate'] = curdata['duedate']
        return feeplandata,feepaymentdatesrec
    else:
        doc = db.collection(entitytype).document(entityid).collection("SIMPLEFEEPLAN").document(feeplan).get()
        
        feepdata =None
        if doc.exists:
            feepdata=doc.to_dict()
        return feepdata,None

def sessionVR_OfferingProcessing(transaction,db, sessionterm,virtualroomname,entitytype,entityid,idcardnum,addremoveflag,offeringsschedule,profileid,addvraary,remvraary,addofraary,remofraary):
    if virtualroomname is not None:
        virtualroomchannel =getChannelFromVirtualRoomNewFormat(transaction,db, virtualroomname,entitytype,entityid)
        addvraary,remvraary,addofraary,remofraary =returnUserRecordForVR_OfferingSch(virtualroomchannel,profileid,True,addremoveflag,idcardnum,addvraary,remvraary,addofraary,remofraary,sessionterm,virtualroomname,None)

    if offeringsschedule is not None:
        for offsch in offeringsschedule:
            
            channel = getChannelFromOfferingNewFormat(transaction,db, offsch,entitytype,entityid)
            if channel ==None:
                return None,None,None,None, "Channel not found in offering sch" + id
            addvraary,remvraary,addofraary,remofraary =returnUserRecordForVR_OfferingSch(channel,profileid,False,addremoveflag,idcardnum,addvraary,remvraary,addofraary,remofraary,sessionterm,None,offsch)
    return addvraary,remvraary,addofraary,remofraary,None



#changes allowed are - Feeplantype and feeplan data change
#if feeplan is changing, make sure that no half paid feepayment exists,if exist - ask user to cancel it,or complete it


def UserRegistrationFeeAndSessionInformationDelete(transaction,db,idcardnum,sessionterm,entitytype,entityid):
    feeplandata=None
    feepaymentdatesrec =None
    requireFeeProcess =False
    usersessioncompletedata =None
    registrationdata=None
    feeplaninnewdata=False
    feeplan =None
    profileid ="S_R_"+entityid
    transportregdata =None
    usersessioncompletedata,error=getUserSessionRegistrationData(transaction,db,entitytype,entityid,idcardnum+"@"+sessionterm)
    virtualroomname=usersessioncompletedata["virtualroom"]
    offeringsschedule=usersessioncompletedata["offeringsschedule"]
    regdata,error=getUserRegistrationData(transaction,db,entitytype,entityid,idcardnum)

    if regdata is None:
        return None,error
    

    userrecordadd ={}
    userdataadd={}
    addvraary=[]
    remvraary=[]
    addofraary=[]
    remofraary=[]    
    addremoveflag=False #as we adding vr or offering schedule
    addvraary,remvraary,addofraary,remofraary,error=sessionVR_OfferingProcessing(transaction,db, sessionterm,virtualroomname,entitytype,entityid,idcardnum,addremoveflag,offeringsschedule,profileid,addvraary,remvraary,addofraary,remofraary)
    if len(remvraary) >0:
        userdataadd[profileid+".channels_vr"]= ArrayRemove(remvraary)
    if len(remofraary) >0:
        userdataadd[profileid+".channels_oc"]= ArrayRemove(remofraary)
    updateUserProfileForRegistration(transaction,db,regdata['appuserid'],regdata['gaurdian1appuserid'],regdata['gaurdian2appuserid'],userdataadd)
        
    paymentdateref = db.collection(entitytype).document(entityid).collection("USERREGISTRATIONPAYMENTDATES").document(idcardnum)
    print(feepaymentdatesrec)
    #EMpty the contents of the payment details
    transaction.set(paymentdateref,{})
    #update listofregisterid in virtualroom
    #registration_ref.collection("SESSIONTERM").document(sessionname).collection("VIRTUALROOMS").document(virtualroom).update({"listofregisterid":ArrayUnion([{"name":name,"rollnumber":rollnumber,"idcardnum":idcardnumstr,"photourl":"abc"}])})
    vrroomref = db.collection('SERVICEPROVIDERINFO').document(entityid).collection("SESSIONTERM").document(sessionterm).collection("STUDATAVR").document(virtualroomname)
    myd={"name":regdata["name"],"id":idcardnum,"rno":usersessioncompletedata["rollnumber"],"photourl":regdata["photo"]}
    print(myd)
    updatedata={"listofregisterid":ArrayRemove([myd])}
    print(updatedata)
    transaction.update(vrroomref,updatedata)

    #start updating information
    if usersessioncompletedata["tripregid"]  is not None:  
        transportregdata={}
        transportregdata["status"]="closed"
        transportregdata["enddate"]=getutctimestampfornow()
        tref = db.collection(entitytype).document(entityid).collection("TRIPREGISTRATION").document(usersessioncompletedata["tripregid"])
        transaction.update(tref,transportregdata)        



    if offeringsschedule is not None:
        for offsch in offeringsschedule:

            ocref = db.collection('SERVICEPROVIDERINFO').document(entityid).collection("SESSIONTERM").document(sessionterm).collection("OFFERINGSCHEDULE").document(offsch)
            updatedata={"listofregisterid":ArrayRemove([{"name":regdata["name"],"id":idcardnum,"vr":virtualroomname,"photourl":regdata["photo"]}])}
            print (updatedata)
            transaction.update(ocref,updatedata)


    #do session registration
    userregref = db.collection('SERVICEPROVIDERINFO').document(entityid).collection("USERSESSIONREGISTRATION").document(idcardnum+"@"+sessionterm)
    transaction.update(userregref,{"dateofleaving":getutctimestampfornow()})

    return userregref.id, None

def getCounterFromAssignment(transaction,db,asgid,countername ,entitytype,entityid):
    docpath =entitytype+'/'+entityid+'/ASSIGNMENT/'+asgid
    print(docpath)
    doc1 = transaction.get(db.collection('SERVICEPROVIDERINFO').document(entityid).collection("ASSIGNMENT").where(firestore1.field_path.FieldPath.document_id(), "==", db.document(docpath)).select([countername]))
    setflag=False
    counterval=None
    for x in doc1:
        if x.exists:
            m = x.to_dict()
            setflag=True
            if countername in m:
                counterval = m[countername]
    return counterval


def AttachAssignmentOperationCreate(transaction,db,asgid,sessionterm,data,entitytype,entityid):
    doc1= db.collection(entitytype).document(entityid).collection("SESSIONTERM").document(sessionterm).collection("VRASSIGNMENT").document()
    doc2= db.collection(entitytype).document(entityid).collection("SESSIONTERM").document(sessionterm).collection("VRASSIGNMENTSCORE").document(doc1.id)
    data["vrasgscorid"]=doc2.id
    transaction.set(doc2,data)
    transaction.set(doc1,{})




def SaveScoreToAssignment(transaction,db,sessionterm,asgid,data,idcardnum,entitytype,entityid):
    docpath =entitytype+'/'+entityid+'/SESSIONTERM/'+sessionterm+'/VRASSIGNMENT/'+asgid
    print(docpath)
    doc1 = transaction.get(db.collection(entitytype).document(entityid).collection("SESSIONTERM").document(sessionterm).collection("VRASSIGNMENT").where(firestore1.field_path.FieldPath.document_id(), "==", db.document(docpath)).select(["assignmentid"]))
    assignmentid=None
    
    for x in doc1:
        if x.exists:
            m = x.to_dict()
            assignmentid = m["assignmentid"]

    docpath =entitytype+'/'+entityid+'/SESSIONTERM/'+sessionterm+'/VRASSIGNMENTSCORE/'+asgid
    doc1 = transaction.get(db.collection(entitytype).document(entityid).collection("SESSIONTERM").document(sessionterm).collection("VRASSIGNMENTSCORE").where(firestore1.field_path.FieldPath.document_id(), "==", db.document(docpath)).select(["test"]))
    VRASSIGNMENTSCOREexist=False
    for x in doc1:
        if x.exists:
            VRASSIGNMENTSCOREexist=True


    if assignmentid ==None:
        return None,"Assignment Master doesnt exist"

    doc1 =db.collection('SERVICEPROVIDERINFO').document(entityid).collection("FINALASSIGNMENT").document(assignmentid).get(transaction=transaction)
    assignmentdata=None
    if doc1.exists:
        assignmentdata = doc1.to_dict()
    else:
        return None,"Assignment master data doesnt exist"

    autoevalquestion =0
    autoevalcorans=0
    autoevalwroans=0
    autoevalunattempted=0
    automctotalscore=0
    autoevalscore=0
    totalquestions = assignmentdata["totalquestion"]
    stuansdict={}
    for ians in data:
        stuansdict[ians["id"]] =ians

    for id in range(1,totalquestions,1):
        qid ="q" + str(id)
        questiondata= assignmentdata[qid]
        qtype=None
        if "questiontype" not in questiondata:
            continue
        questiontype =questiondata["questiontype"]
        if questiontype !="mc":
            continue
        autoevalquestion=autoevalquestion+1
        answers = questiondata["answers"]
        score =questiondata["score"]
        automctotalscore=automctotalscore+score
        stu_ans = None
        if qid in stuansdict:
            stu_ans=stuansdict[qid]
        else:
            autoevalunattempted=autoevalunattempted+1
            stu_ans["eval"]="u"
        
        stu_choices = stu_ans["choices"]

        if answers ==stu_choices:
            autoevalcorans=autoevalcorans+1
            autoevalscore=autoevalscore+score
            stu_ans["eval"]="r"
        else:
            autoevalwroans=autoevalwroans+1
            stu_ans["eval"]="w"

    mscoredata={}
    mscoredata["answers"] =data
    mscoredata["mcquestion"]=autoevalquestion
    mscoredata["mctotalscore"]=automctotalscore
    mscoredata["mcevalscore"]=autoevalscore
    mscoredata["mcrightans"]=autoevalcorans
    mscoredata["mcwrongans"]=autoevalwroans
    mscoredata["mcunattempted"]=autoevalunattempted

    doc1= db.collection(entitytype).document(entityid).collection("SESSIONTERM").document(sessionterm).collection("VRASSIGNMENT").document(asgid)
    doc2= db.collection(entitytype).document(entityid).collection("SESSIONTERM").document(sessionterm).collection("VRASSIGNMENTSCORE").document(asgid)

    #answers 
    transaction.update(doc1,{"hasscore":True})


    if VRASSIGNMENTSCOREexist:
        transaction.update(doc2,{idcardnum:mscoredata})
    else:
        transaction.set(doc2,{idcardnum:mscoredata})
    return asgid,None




def PublishAssignment(transaction,db,asgid,data,entitytype,entityid):
    doc1= db.collection(entitytype).document(entityid).collection("ASSIGNMENT").document(asgid).get(transaction=transaction)
    mdic=None
    if doc1.exists:
        mdic = doc1.to_dict()
    else:
        return None,"Assignment doesnt exist"

    asgref= db.collection(entitytype).document(entityid).collection("ASSIGNMENT").document(asgid)
    asgfinalref=db.collection(entitytype).document(entityid).collection("FINALASSIGNMENT").document()

    curversion = None
    versionhis = None

    if "curv" in mdic:
        curversion=mdic["curversion"] +1
    else:
        curversion=1
    qts =None
    if "qts" in mdic:
        qts=mdic["qts"] 
    else:
        qts=0
    asgdata={}
    asgdata["curversion"] =curversion
    asgdata["versionhis"] =ArrayUnion([{"fid":asgfinalref.id,"qts":qts,"v":curversion}])
    transaction.update(asgref,asgdata)
    
    mdic["aid"] =asgid
    mdic["copyversion"]=curversion
    transaction.set(asgfinalref,mdic)
    return asgid,None

def CreateAssignmentQuestion(transaction,db,asgid,data,entitytype,entityid):
    mval = getCounterFromAssignment(transaction,db,asgid,"totalquestion",entitytype,entityid)
    mdata={}

    for data1 in data:
        qid =data1["qid"]
        qdata=data1
        if qid==None:
            if mval==None:
                mval =1
            else:
                mval= mval+1

            mdata["q"+str(mval)]=qdata
        else:
            mdata[qid]=qdata

    mdata["totalquestion"]=mval
    mdata["qts"] =firestore.SERVER_TIMESTAMP
    docref = db.collection('SERVICEPROVIDERINFO').document(entityid).collection("ASSIGNMENT").document(asgid)
    transaction.update(docref,mdata)
    return asgid,None

def CreateAssignmentStudyMaterial(transaction,db,asgid,data,entitytype,entityid):
    mval = getCounterFromAssignment(transaction,db,asgid,"smcount",entitytype,entityid)
    mdata={}
    for data1 in data:
        sid =data1["smid"]
        qdata=data1
        if sid==None:
            if mval==None:
                mval =1
            else:
                mval= mval+1
            mdata["sm"+str(mval)]=qdata
        else:
            mdata[sid]=qdata

    mdata["smcount"]=mval
    docref = db.collection('SERVICEPROVIDERINFO').document(entityid).collection("ASSIGNMENT").document(asgid)
    transaction.update(docref,mdata)
    return asgid,None






# Addition removal of fee route
def UserRegistrationFeeAndSessionInformationUpdate(transaction,db,olddata,newdata,idcardnum,sessionterm,entitytype,entityid):
    feeplandata=None
    feepaymentdatesrec =None
    requireFeeProcess =False
    usersessioncompletedata =None
    registrationdata=None
    feeplaninnewdata=False
    feeplan =None
    profileid ="S_R_"+entityid
    usersessioncompletedata,error=getUserSessionRegistrationData(transaction,db,entitytype,entityid,idcardnum+"@"+sessionterm)
    feeplantype=None
    if "feeplantype" in newdata:
        feeplantype =newdata["feeplantype"]
    else:
        feeplantype = usersessioncompletedata["feeplantype"]

    if usersessioncompletedata is None:
        return None,error
    registrationdata,error=getUserRegistrationData(transaction,db,entitytype,entityid,idcardnum)

    if registrationdata is None:
        return None,error

    
    if "feeplan" in newdata or 'startperiod' in newdata or 'feeplantype' in newdata:

        feeplantype = usersessioncompletedata['feeplantype']
        feeplan= usersessioncompletedata['feeplan']
        startperiod = usersessioncompletedata['startperiod']
        if 'feeplan' in newdata :
            feeplan =newdata['feeplan']
        if 'feeplantype' in newdata :
            feeplantype= newdata['feeplantype']
        if 'startperiod' in newdata :
            startperiod = newdata['startperiod']
        print (newdata['startperiod'])
                                        
        feeplandata,feepaymentdatesrec =processFeePlanSessionRegistration(transaction,db,feeplantype,registrationdata,startperiod,idcardnum,feeplan,registrationdata["appuserid"],entitytype,registrationdata["dateofjoining"],entityid,False)
        newdata["feeplan"]=feeplandata
        requireFeeProcess =True

    ####CHECKS for TRANSPORT RELATED CHANGES########
    transportaction= None
    transportregdata=None
    #if I - it means new record insertion, if U - update
    if "allocatedtransportroute" in newdata: 
        ###Check if initially user didnt have transport but now opted for transport
        if (olddata["allocatedtransportroute"] is None or not olddata["allocatedtransportroute"])  and (newdata["allocatedtransportroute"] is not None and newdata["allocatedtransportroute"]):
            if registrationdata is None:
                registrationdata,error=getUserRegistrationData(transaction,db,entitytype,entityid,idcardnum)
                if registrationdata is None:
                    return None,error
            transportregdata=TripLongTermRideRegistration(registrationdata,entitytype,entityid)
            transportaction="I"        
           ####Check if transport facility is removed for the new user, but was present in olddata
        elif (olddata["allocatedtransportroute"] is not None and olddata["allocatedtransportroute"]) and (newdata["allocatedtransportroute"] is  None or not newdata["allocatedtransportroute"]):
            transportregdata={}
            transportregdata["status"]="closed"
            transportregdata["enddate"]=getutctimestampfornow()
            newdata["tripregid"]=None
            transportaction="U"       

    ###Check for Virtual room and offering schedule#######
    userdataadd =None
    userdatadel=None
    todeloffering=None
    toaddoffering=None
    toaddvr=None
    todelvr = None
    dovrprocessing=False
    doofrprocess=False

    if "virtualroom" in newdata or "offeringsschedule" in  newdata:

        if "offeringsschedule" in newdata:
            todeloffering,toaddoffering = DiffAddSubSimple(olddata["offeringsschedule"], newdata["offeringsschedule"])
            doofrprocess =True           
        if "virtualroom" in newdata:
            toaddvr= newdata["virtualroom"]
            todelvr =olddata["virtualroom"]
            dovrprocessing=True
            #if virtual room changes , we need to also change the vr for the existing offering
            if "offeringsschedule" not in newdata:
                todeloffering=usersessioncompletedata["offeringsschedule"]
                toaddoffering=usersessioncompletedata["offeringsschedule"]
                doofrprocess=True


        userdataadd ={}
        userdatadel={}
        addvraary=[]
        remvraary=[]
        addofraary=[]
        remofraary=[]

        addremoveflag=False #as we removing vr or offering schedule
        addvraary,remvraary,addofraary,remofraary,error=sessionVR_OfferingProcessing(transaction,db, sessionterm,todelvr,entitytype,entityid,idcardnum,addremoveflag,todeloffering,profileid,addvraary,remvraary,addofraary,remofraary)
        addremoveflag=True #as we adding vr or offering schedule
        addvraary,remvraary,addofraary,remofraary,error=sessionVR_OfferingProcessing(transaction,db, sessionterm,toaddvr,entitytype,entityid,idcardnum,addremoveflag,toaddoffering,profileid,addvraary,remvraary,addofraary,remofraary)
        if len(remvraary) >0:
            userdatadel[profileid+".channels_vr"]= ArrayRemove(remvraary)
        if len(addvraary) >0:
            userdataadd[profileid+".channels_vr"]= ArrayUnion(addvraary)
        if len(remofraary) >0:
            userdatadel[profileid+".channels_oc"]= ArrayRemove(remofraary)
        if len(addofraary) >0:
            userdataadd[profileid+".channels_oc"]= ArrayUnion(addofraary)


    ###Add/Update transaction processing 

    ###Feeplan part processing
    if requireFeeProcess:
        paymentdateref = db.collection(entitytype).document(entityid).collection("USERREGISTRATIONPAYMENTDATES").document(idcardnum)
        print(feepaymentdatesrec)
        transaction.set(paymentdateref,feepaymentdatesrec)

    ###VR processing
    if dovrprocessing:
        #delete processing
        vrroomref = db.collection('SERVICEPROVIDERINFO').document(entityid).collection("SESSIONTERM").document(sessionterm).collection("STUDATAVR").document(todelvr)
        myd={"name":registrationdata["name"],"id":idcardnum,"rno":usersessioncompletedata["rollnumber"],"photourl":registrationdata["photo"]}
        print(myd)
        updatedata={"listofregisterid":ArrayRemove([myd])}
        print(updatedata)
        transaction.set(vrroomref,updatedata,merge=True)

        vrroomref = db.collection('SERVICEPROVIDERINFO').document(entityid).collection("SESSIONTERM").document(sessionterm).collection("STUDATAVR").document(toaddvr)
        myd={"name":registrationdata["name"],"id":idcardnum,"rno":usersessioncompletedata["rollnumber"],"photourl":registrationdata["photo"]}
        print(myd)
        updatedata={"listofregisterid":ArrayUnion([myd])}
        print(updatedata)
        transaction.set(vrroomref,updatedata,merge=True)
    ###Offering processing 
    if doofrprocess:
        virtualroomname= usersessioncompletedata["virtualroom"]
        #del processing
        if todeloffering is not None:
            for offsch in todeloffering:
                
                ocref = db.collection('SERVICEPROVIDERINFO').document(entityid).collection("SESSIONTERM").document(sessionterm).collection("STUDATAOFR").document(offsch)
                updatedata={"listofregisterid":ArrayRemove([{"name":registrationdata["name"],"id":idcardnum,"vr":virtualroomname,"photourl":registrationdata["photo"]}])}
                print (updatedata)
                transaction.set(ocref,updatedata,merge=True)
        #add processing
        if toaddoffering is not None:
            virtualroomname= usersessioncompletedata["virtualroom"]
            if "virtualroom" in newdata:
                virtualroomname= newdata["virtualroom"]
            for offsch in toaddoffering:
                
                ocref = db.collection('SERVICEPROVIDERINFO').document(entityid).collection("SESSIONTERM").document(sessionterm).collection("STUDATAOFR").document(offsch)
                updatedata={"listofregisterid":ArrayUnion([{"name":registrationdata["name"],"id":idcardnum,"vr":virtualroomname,"photourl":registrationdata["photo"]}])}
                print (updatedata)
                transaction.set(ocref,updatedata,merge=True)
    ###transport update processing
    #update transport regdata
    if transportregdata is not None:
        if transportaction =="U":
            tref = db.collection(entitytype).document(entityid).collection("TRIPREGISTRATION").document(usersessioncompletedata["tripregid"])
            transaction.update(tref,transportregdata)
        else:
            tref = db.collection(entitytype).document(entityid).collection("TRIPREGISTRATION").document()
            transaction.set(tref,transportregdata)
            newdata["tripregid"]=tref.id
    #update user profile data
    if userdatadel is not None and len(userdatadel) >0:
        updateUserProfileForRegistration(transaction,db,registrationdata['appuserid'],registrationdata['gaurdian1appuserid'],registrationdata['gaurdian2appuserid'],userdatadel)

    if userdataadd is not None and len(userdataadd) >0:
        updateUserProfileForRegistration(transaction,db,registrationdata['appuserid'],registrationdata['gaurdian1appuserid'],registrationdata['gaurdian2appuserid'],userdataadd)

    ### update session registration 
    userregref = db.collection('SERVICEPROVIDERINFO').document(entityid).collection("USERSESSIONREGISTRATION").document(idcardnum+"@"+sessionterm)
    transaction.update(userregref,newdata)
    return userregref.id, None







def UserRegistrationFeeAndSessionInformationInsert(transaction,db,usersessioninformation,entitytype,entityid):
    
    if "idcardnum" not in usersessioninformation or  "virtualroom" not in usersessioninformation or "offeringsschedule" not in usersessioninformation or  "feeplantype" not in usersessioninformation or 'feeplan' not in usersessioninformation or 'activesession' not in usersessioninformation:
        return None, "mandatory parameter missing one or more  - idcardnum,virtualroom,feeplantype,feeplan,offeringsschedule,activesession"
    idcardnum=usersessioninformation["idcardnum"]
    virtualroomname=usersessioninformation["virtualroom"]
    feeplantype=usersessioninformation["feeplantype"]
    feeplan=usersessioninformation["feeplan"]
    offeringsschedule=usersessioninformation["offerings"]
    sessionterm=usersessioninformation["activesession"]
    startperiod =None
    if 'startperiod' in usersessioninformation and usersessioninformation['startperiod'] is not None:
        startperiod = usersessioninformation['startperiod']
    feepaymentrecord= {}
    profileid ="S_R_"+entityid

    regdata,error=getUserRegistrationData(transaction,db,entitytype,entityid,idcardnum)
    if regdata is None:
        return None,error

    transportregdata =None
    if "allocatedtransportroute" in usersessioninformation and usersessioninformation["allocatedtransportroute"] is not None and usersessioninformation["allocatedtransportroute"]:
        transportregdata=TripLongTermRideRegistration(regdata,entitytype,entityid)        
    

    feepaymentrecord["qrcode"] =regdata["appuserid"]

    #fee policy and feerecord data
    feeplandata,feepaymentdatesrec =processFeePlanSessionRegistration(transaction,db,feeplantype,usersessioninformation,startperiod,idcardnum,feeplan,regdata["appuserid"],entitytype,regdata["dateofjoining"],entityid,False)

    #add the check for feecheck via 

    #get the channel for offering schedule and virtual room
    
    userrecordadd ={}
    userdataadd={}
    addvraary=[]
    remvraary=[]
    addofraary=[]
    remofraary=[]    
    addremoveflag=True #as we adding vr or offering schedule
    addvraary,remvraary,addofraary,remofraary,error=sessionVR_OfferingProcessing(transaction,db, sessionterm,virtualroomname,entitytype,entityid,idcardnum,addremoveflag,offeringsschedule,profileid,addvraary,remvraary,addofraary,remofraary)
    if len(addvraary) >0:
        userdataadd[profileid+".channels_vr"]= ArrayUnion(addvraary)
    if len(addofraary) >0:
        userdataadd[profileid+".channels_oc"]= ArrayUnion(addofraary)


    #update and add record

    #Updaing userprofiles
    updateUserProfileForRegistration(transaction,db,regdata['appuserid'],regdata['gaurdian1appuserid'],regdata['gaurdian2appuserid'],userdataadd)




    #update transport regdata
    if transportregdata is not None:
        tref = db.collection(entitytype).document(entityid).collection("TRIPREGISTRATION").document()
        transaction.set(tref,transportregdata)
        usersessioninformation["tripregid"]=tref.id
    else:
        usersessioninformation["tripregid"]=None

    #save payment date
    paymentdateref = db.collection(entitytype).document(entityid).collection("USERREGISTRATIONPAYMENTDATES").document(idcardnum)
    print(feepaymentdatesrec)
    transaction.set(paymentdateref,feepaymentdatesrec)
    #update listofregisterid in virtualroom
    #registration_ref.collection("SESSIONTERM").document(sessionname).collection("VIRTUALROOMS").document(virtualroom).update({"listofregisterid":ArrayUnion([{"name":name,"rollnumber":rollnumber,"idcardnum":idcardnumstr,"photourl":"abc"}])})
    vrroomref = db.collection('SERVICEPROVIDERINFO').document(entityid).collection("SESSIONTERM").document(sessionterm).collection("STUDATAVR").document(virtualroomname)
    myd={"name":regdata["name"],"id":idcardnum,"rno":usersessioninformation["rollnumber"],"photourl":regdata["photo"]}
    print(myd)
    updatedata={"listofregisterid":ArrayUnion([myd])}
    print(updatedata)
    transaction.set(vrroomref,updatedata,merge=True)

    #update offering sch list of registerid

    if offeringsschedule is not None:
        for offsch in offeringsschedule:
            #id = offsch["id"]
            ocref = db.collection('SERVICEPROVIDERINFO').document(entityid).collection("SESSIONTERM").document(sessionterm).collection("STUDATAOFR").document(offsch)
            updatedata={"listofregisterid":ArrayUnion([{"name":regdata["name"],"id":idcardnum,"vr":virtualroomname,"photourl":regdata["photo"]}])}
            transaction.set(ocref,updatedata,merge=True)


    #do session registration
    userregref = db.collection('SERVICEPROVIDERINFO').document(entityid).collection("USERSESSIONREGISTRATION").document(idcardnum+"@"+sessionterm)
    transaction.set(userregref,usersessioninformation)
    return userregref.id, None

def TripLongTermRideRegistration(regdata,entitytype,entityid):
    mydata={}
    mydata["gaurdian1relation"]=regdata["gaurdian1relation"]
    mydata["gaurdian1name"]=regdata["gaurdian1name"]
    mydata["gaurdian1email"]=regdata["gaurdian1email"]
    mydata["gaurdian1phone"]=regdata["gaurdian1phone"]
    mydata["gaurdian1appuserid"]=regdata["gaurdian1appuserid"]
    mydata["gaurdian2relation"]=regdata["gaurdian2relation"]
    mydata["gaurdian2name"]=regdata["gaurdian2name"]
    mydata["gaurdian2email"]=regdata["gaurdian2email"]
    mydata["gaurdian2phone"]=regdata["gaurdian2phone"]
    mydata["gaurdian2appuserid"]=regdata["gaurdian2appuserid"]
    mydata["state"]=regdata["state"]
    mydata["town"]=regdata["town"]
    mydata["addressline"]=regdata["addressline"]
    mydata["zipcode"]=regdata["zipcode"]
    mydata["email"]=regdata["email"]
    mydata["phone"]=regdata["phone"]
    mydata["appuserid"]=regdata["appuserid"]
    mydata["gender"]=regdata["gender"]
    mydata["isadult"]=regdata["isadult"]
    mydata["name"]=regdata["name"]
    mydata["dob"]=regdata["dob"]
    mydata["photo"]=regdata["photo"]
    mydata["gender"]=regdata["gender"]
    mydata["dateofjoining"]=regdata["dateofjoining"]
    mydata["dateofleaving"]=regdata["dateofleaving"]
    mydata["idcardnum"]=regdata["idcardnum"]
    mydata["entityid"]=entityid
    mydata["entitytype"]=entitytype
    return mydata



def getRegistrationNumber(transaction,db,entitytype,entityid,prefix,startcount):
    doc1= db.collection(entitytype).document(entityid).collection("COUNTERS").document("id").get(transaction=transaction);
    setflag=True
    counterval=startcount
    
    if doc1.exists:
        mdic = doc1.to_dict()
        if "id" in mdic:
            counterval=mdic["id"]
        if "pr" in mdic:
            curprefix=mdic["pr"]
        setflag=True
    docref=db.collection(entitytype).document(entityid).collection("COUNTERS").document("id")
    if setflag:
        transaction.set(docref,{"id":counterval+1,"pr":prefix})
    else:
        transaction.update(docref,{"id":counterval+1})
    
    return prefix +str(counterval),None

def getRollNumberNewFormat(transaction,db,entitytype,entityid,virtualroomname,sessionterm):
    docpath =entitytype+'/'+entityid+'/SESSIONTERM/'+sessionterm+'/VIRTUALROOMS/'+virtualroomname
    print(docpath)
    #doc1 = transaction.get(db.collection('SERVICEPROVIDERINFO').document(entityid).collection("SESSIONTERM").document(sessionterm).collection("VIRTUALROOMS").where(firestore1.field_path.FieldPath.document_id(), "==", db.document(docpath)).select(['runningnumber']))
    docref =db.collection('SERVICEPROVIDERINFO').document(entityid).collection("SESSIONTERM").document(sessionterm).collection("STUDATAVR").document(virtualroomname)
    x = docref.get(['runningnumber'],transaction=transaction)
    setflag=False
    runningnumber=1

    if x.exists:
        m = x.to_dict()
        setflag=setflag+1
        if "runningnumber" in m:
            runningnumber = m["runningnumber"]
    else:
        docref =db.collection('SERVICEPROVIDERINFO').document(entityid).collection("SESSIONTERM").document(sessionterm).collection("VIRTUALROOMS").document(virtualroomname)
        transaction.set(docref,{"runningnumber":2},merge=True)
        return 1,None

    docref=db.collection('SERVICEPROVIDERINFO').document(entityid).collection("SESSIONTERM").document(sessionterm).collection("VIRTUALROOMS").document(virtualroomname)
    if setflag:
        transaction.update(docref,{"runningnumber":runningnumber+1})
    else:
        return None, "Virtual room doesnt exist"
    return runningnumber,None

def getRollNumber(transaction,db,entitytype,entityid,virtualroomname,sessionterm):
    docpath =entitytype+'/'+entityid+'/SESSIONTERM/'+sessionterm+'/VIRTUALROOMS/'+virtualroomname
    print(docpath)
    doc1 = transaction.get(db.collection('SERVICEPROVIDERINFO').document(entityid).collection("SESSIONTERM").document(sessionterm).collection("VIRTUALROOMS").where(firestore1.field_path.FieldPath.document_id(), "==", db.document(docpath)).select(['runningnumber']))
    setflag=False
    runningnumber=1
    for x in doc1:
        if x.exists:
            m = x.to_dict()
            setflag=setflag+1
            if "runningnumber" in m:
                runningnumber = m["runningnumber"]
            else:
                docref =db.collection('SERVICEPROVIDERINFO').document(entityid).collection("SESSIONTERM").document(sessionterm).collection("VIRTUALROOMS").document(virtualroomname)
                transaction.set(docref,{"runningnumber":2},merge=True)
                return 1

    docref=db.collection('SERVICEPROVIDERINFO').document(entityid).collection("SESSIONTERM").document(sessionterm).collection("VIRTUALROOMS").document(virtualroomname)
    if setflag:
        transaction.update(docref,{"runningnumber":runningnumber+1})
    else:
        return None, "Virtual room doesnt exist"
    return runningnumber,None


def  checkandcreateuser(username, emailaddr, phonenumber,defaultpassword):
    print(emailaddr)
    try:
        user1 = auth.get_user_by_email(emailaddr)
        return user1,True
    except Exception:
        print(traceback.format_exc())
    
    passw= 'secretPassword'
    if(defaultpassword is not None):
        passw=defaultpassword

    user1 = auth.create_user(
    email=emailaddr,
    email_verified=False,
    password=passw,
    display_name=username,
    #photo_url='',
    disabled=False)
    return user1,True

def storeTokenForUser(db,transaction,appuserid,roles, residentunitlist,entityid,entitytype,token):
    mydata={}
    mydata["presence"]=token
    db.collection("USER").document(appuserid).update(mydata)
    return token;

    batch = db.batch()
    if roles is not None:
        if "security" in roles:
            batch.update(db.collection(entitytype).document(entityid).collection(u'SECURITYTOKEN').document("security"),{appuserid:token})
        if "manager" in roles:
            batch.update(db.collection(entitytype).document(entityid).collection(u'SECURITYTOKEN').document("manager"),{appuserid:token})
        if "staff" in roles:
            batch.update(db.collection(entitytype).document(entityid).collection(u'SECURITYTOKEN').document("staff"),{appuserid:token})
        if "management" in roles:
            batch.update(db.collection(entitytype).document(entityid).collection(u'SECURITYTOKEN').document("management"),{appuserid:token})

    if residentunitlist is not None:
        for ru in residentunitlist:
            batch.update(db.collection(entitytype).document(entityid).collection(u'UNITGRPMEM').document(ru["rd"]),{appuserid:token})

    batch.update(db.collection("USERS").document(appuserid),{"fcmtoken":token})
    batch.commit()


def sendFCMMessageTo(tokenlist, mdata):
    print (tokenlist)
    print(mdata)
    if (len(tokenlist) == 1):
        message = messaging.Message( data=mdata, token=tokenlist[0],)
        response = messaging.send(message)
        print (response)
        return response
    elif (len(tokenlist) > 1):
        message = messaging.MulticastMessage(data={'score': '850', 'time': '2:45'},tokens=tokenlist,)
        response = messaging.send_multicast(message)
        print (response)
        return response


def gettokenforActiveStaff(managerstafftype, securitystafftype,simplestaff,db,entitytype,entityid):
    db.collection(entitytype).document(entityid).collection(u'SHIFTPLAN').where("isactive","==",True).stream()
    

def getPresenceTokenFromUserProfile(db,useridlist):
    registrationtokenlist=[]
    registrationtokenwithid={}
    abdf =db.get_all(useridlist)#,['presence']) #if no field parameter is given data comes under subfield data
    docs=[]
    for adoc in abdf:
        if adoc is not None:
            bdoc = adoc.to_dict()
            if  'presence' in bdoc:
                registrationtokenlist.append(bdoc['presence'])
                registrationtokenwithid[adoc.id]=bdoc['presence']

    return registrationtokenlist,registrationtokenwithid
    

def getFCMtoken(db,entitytype,entityid,wantmanager,wantmanagement,wantsecurity,wantstaff, residentdetailid):
    registrationtokenlist=[]
    registrationtokenwithid={}
    if residentdetailid is not None:
        docref = db.collection(entitytype).document(entityid).collection(u'UNITGRPMEM').document(residentdetailid).get(['fm'])
        if docref.exists:
            mdoc =docref.to_dict()
            if 'fm' in mdoc:
                docreflist=[]
                for i in mdoc['fm']:
                    docreflist.append(db.collection("USERS").document(i))
                if len(docreflist) > 0:
                    registrationtokenlist,registrationtokenwithid=getPresenceTokenFromUserProfile(db,docreflist)


    if wantsecurity is  True:
        docref = db.collection(entitytype).document(entityid).collection(u'SECURITYTOKEN').document("security").get()
        if docref.exists:
            mdoc =docref.to_dict()
            for key in mdoc:
                registrationtokenwithid[key] =mdoc[key]
                registrationtokenlist.append(mdoc[key])

    if wantmanager is  True:
        docref = db.collection(entitytype).document(entityid).collection(u'SECURITYTOKEN').document("manager").get()
        if docref.exists:
            mdoc =docref.to_dict()
            for key in mdoc:
                registrationtokenwithid[key] =mdoc[key]
                registrationtokenlist.append(mdoc[key])
    if wantmanagement is  True:
        docref = db.collection(entitytype).document(entityid).collection(u'SECURITYTOKEN').document("management").get()
        if docref.exists:
            mdoc =docref.to_dict()
            for key in mdoc:
                registrationtokenwithid[key] =mdoc[key]
                registrationtokenlist.append(mdoc[key])

    if wantstaff is  True:
        docref = db.collection(entitytype).document(entityid).collection(u'SECURITYTOKEN').document("staff").get()
        if docref.exists:
            mdoc =docref.to_dict()
            for key in mdoc:
                registrationtokenwithid[key] =mdoc[key]
                registrationtokenlist.append(mdoc[key])

    return registrationtokenlist ,registrationtokenwithid


def EntityCreationAction(batch,db,entitytype, entityref,entityname,createdbyappuser,entitydata):
    channellist =[]
    entityid =entityref.id
    if entitytype=="COMPLEXES":
       channel1 = createchannel(db,"Resident",entityid,"complex","internalnotification",["owner","staff","manager","security","resident","management"],["manager","security""staff","management"],batch)
       channel2 = createchannel(db,"Owner",entityid,"complex","internalnotification",["owner","staff","manager","security","management"],["manager","security""staff","management"],batch)
       channel3 = createchannel(db,entityname+"_chat",entityid,"complex","internalgroupchat",["manager","owner","resident","management"],["manager","management"],batch)
       channellist.append(channel1)
       channellist.append(channel2)
       channellist.append(channel3)
	   
    else:	
       channel1 = createchannel(db,"Internal",entityid,"serviceprovider","internalnotification",["staff"],["manager","management"],batch)
       channel2 = createchannel(db,"Announcement",entityid,"serviceprovider","externalnotification",["all"],["manager","management"],batch)
       channellist.append(channel1)
       channellist.append(channel2)

    internaldata={}
    internaldata["users"] =[]
    internaldata["rusers"] =[]
    internaldata_ref = db.collection(entitytype).document(entityid).collection(u"internaldata").document("first")
    batch.set(internaldata_ref,internaldata)

    sec_ref = db.collection(entitytype).document(entityid).collection(u"SECURITYTOKEN").document("security")
    batch.set(sec_ref,{})

    manager_ref = db.collection(entitytype).document(entityid).collection(u"SECURITYTOKEN").document("manager")
    batch.set(manager_ref,{})

    management_ref = db.collection(entitytype).document(entityid).collection(u"SECURITYTOKEN").document("management")
    batch.set(management_ref,{})

    staff_ref = db.collection(entitytype).document(entityid).collection(u"SECURITYTOKEN").document("staff")
    batch.set(staff_ref,{})

    lookup_ref = db.collection(entitytype).document(entityid).collection(u"LOOKUPS").document("FIRST")
    mydata={}
    mydata["feeitemlist"]=[]
    mydata["offerings"]=[]
    #mydata["paymentperiodinfo"]=[{"grpname":"quater","paymentperiodname":"TriMonth1","startdate":"1-4-2020","enddate":"30-6-2020"},{"grpname":"quater","paymentperiodname":"TriMonth2","startdate":"1-7-2020","enddate":"31-9-2020"},{"grpname":"quater","paymentperiodname":"TriMonth3","startdate":"1-10-2020","enddate":"31-12-2020"},{"grpname":"quater","paymentperiodname":"TriMonth4","startdate":"1-1-2021","enddate":"31-3-2020"},{"grpname":"month","paymentperiodname":"Month1","startdate":"1-4-2020","enddate":"30-4-2020"},{"grpname":"month","paymentperiodname":"Month2","startdate":"1-5-2020","enddate":"30-5-2020"},{"grpname":"month","paymentperiodname":"Month3","startdate":"1-6-2020","enddate":"30-6-2020"},{"grpname":"month","paymentperiodname":"Month4","startdate":"1-7-2020","enddate":"30-7-2020"},{"grpname":"month","paymentperiodname":"Month5","startdate":"1-8-2020","enddate":"30-8-2020"},{"grpname":"month","paymentperiodname":"Month6","startdate":"1-9-2020","enddate":"30-9-2020"},{"grpname":"month","paymentperiodname":"Month7","startdate":"1-10-2020","enddate":"30-10-2020"},{"grpname":"month","paymentperiodname":"Month8","startdate":"1-11-2020","enddate":"30-11-2020"},{"grpname":"month","paymentperiodname":"Month9","startdate":"1-12-2020","enddate":"30-12-2020"},{"grpname":"month","paymentperiodname":"Month10","startdate":"1-1-2021","enddate":"30-1-2021"},{"grpname":"month","paymentperiodname":"Month11","startdate":"1-2-2021","enddate":"30-2-2021"},{"grpname":"month","paymentperiodname":"Month12","startdate":"1-3-2021","enddate":"30-3-2021"}]
    mydata["paymentperiodinfo"]=[]
    mydata["classperiodinfo"]=[]
    mydata["examterminfo"]=[]
    mydata["sessionterm"]=[]
    mydata["roominfo"]=[]
    mydata["grades"]=[]

    batch.set(lookup_ref,mydata)
    #complexdata_ref = db.collection(entitytype).document(entityid)
    entitydata["channels"]=channellist
    #batch.update(entityref,{"channels":channellist})

    print ("reached end of entity action")

    return entityref.id, None



  
def complex_qrcodeentryServiceReq(qrcode,name,photolink,appuserid,startdate,enddate,entitytype,mtype,unitaddress,servicereqid):
    mydata={}
    mydata["version"] =1
    mydata["qrcode"] =qrcode
    mydata["startdate"] =startdate
    mydata["enddate"] =enddate
    mydata["entytype"] =mtype
    mydata["photolink"] =photolink
    mydata["userid"] =appuserid
    mydata["name"] =name
    mydata["vehiclenumplate"] ="Numplate"
    mydata["iaactive"] =True
    mydata["serversidetimestamp"] =firestore.SERVER_TIMESTAMP
    
    if(mtype is not None and mtype is not "v" and mtype is not "g"):
        mydata["relatedentry"] = [{"mtype":mtype,"startdate":startdate,"enddate":enddate,"isc":True}]
        if (unitaddress is not None):
            mydata["relatedentry"][0]["rd"] =unitaddress
        if (servicereqid is not None):
            mydata["relatedentry"][0]["sr"] =servicereqid

    	
    return mydata

# type - m for staff, r for resident ,  h -homehelp , g - guest/visitor,  
def findmatchedentry(unitaddress,servicereqid,dataarray,mtype):
    matchedVal = None
    othervalList = []
    qrcodecurrent_entry = None
    qrcodecurrent_staff = None
    fieldname ="rd"
    fieldvalue =unitaddress
    if mtype=="h" or mtype=="g" :
        fieldname ="sr"
        fieldvalue =servicereqid
    print (mtype)
    print (fieldname)
    print(fieldvalue)
    if dataarray is None:
        return matchedVal,othervalList,qrcodecurrent_entry,qrcodecurrent_staff
    for item in dataarray:
        if mtype =="r" or mtype=="h"  or mtype=="h" :
            if  fieldname is not None and fieldvalue is not None and  fieldname in item and  item[fieldname] !=fieldvalue:
                othervalList.append(item)
            else:
                matchedVal = item
        elif mtype=="m":
            if "mtype" in item and  item["mtype"] =="m":
                qrcodecurrent_staff =item
                matchedVal = item
            else:
                othervalList.append(item)


        if 	"isc" in item and  item["isc"] ==True:
            qrcodecurrent_entry =item 

    return matchedVal,othervalList,qrcodecurrent_entry,qrcodecurrent_staff


def VirtualRoomChannelOperation(db,operation,ownerinfo,virtualroomchannel,entkey,oldisprimaryval,newprimaryval,transaction,entitytype,entityid,sessionterm,virtualroomname):
    if virtualroomchannel is None:
        return; 
    
    vr ={}
    vr["channel"]=virtualroomchannel
    vr["isp"]=newprimaryval
    vr["st"]=sessionterm
    vr["vr"]=virtualroomname
    

    if operation=="add":
        mydata ={entkey+"."+	"channels_vr":ArrayUnion([vr])}

    elif operation=="remove":
        mydata ={entkey+"."+	"channels_vr":ArrayRemove([vr])}
    elif operation == 'update':
        vr1 ={}
        vr1["channel"]=virtualroomchannel
        vr1["isp"]=oldisprimaryval
        vr1["st"]=sessionterm
        vr1["vr"]=virtualroomname

        mydata ={entkey+"."+	"channels_vr":ArrayRemove([vr1]),entkey+"."+	"channels_vr":ArrayUnion([vr])}

    ##update user profile - virtual room channel
    transaction.update(db.collection("USERS").document(ownerinfo['id']),mydata)

def OfferingScheduleChannelOperation(db,operation,ownerinfo,offeringchannel,entkey,oldisprimaryval,newprimaryval,transaction,entitytype,entityid,ofrschname,sessionterm):
    if offeringchannel is None:
        return 
    
    vr ={}
    vr["channel"]=offeringchannel
    vr["isp"]=newprimaryval
    vr["st"]=sessionterm
    vr["ofr"]=ofrschname
    

    if operation=="add":
        mydata ={entkey+"."+	"channels_oc":ArrayUnion([vr])}

    elif operation=="remove":
        mydata ={entkey+"."+	"channels_oc":ArrayRemove([vr])}


    ##update user profile - virtual room channel
    print (ownerinfo['id'])
    docref =db.collection("USERS").document(ownerinfo['id'])
    transaction.update(docref,mydata)


def converttomap(stringval1,stringval2,stringval3,val):
    myd ={}
    myd[stringval3]=val
    mybcd ={}
    mybcd[stringval2]=myd
    myabc={}
    myabc[stringval1]=mybcd
    return myabc


def SubmitAttendenceVirtualRoom(transaction,db,virtualroomname,offeringkey,sessionterm,mdate,kind,studentInfo,entitytype,entityid):
    datetostr=str(int(mdate))
    dockey=datetostr+"@"+kind
    docref =db.collection(entitytype).document(entityid).collection("SESSIONTERM").document(sessionterm).collection("VIRTUALROOMS").document(virtualroomname).collection("ATTENDENCE").document(dockey).get(transaction=transaction)
    vrref=db.collection(entitytype).document(entityid).collection("SESSIONTERM").document(sessionterm).collection("VIRTUALROOMS").document(virtualroomname).collection("ATTENDENCE").document(dockey)
    myattdata={"adata":{"f_"+kind:{"mdate":mdate,"kind":kind,"sti":studentInfo}}}
    for ib in studentInfo:
        #data[ib["id"]]=ib["val"]
        idref =db.collection(entitytype).document(entityid).collection("SESSIONTERM").document(sessionterm).collection("STUDATA").document(ib["id"]+"@att")
        transaction.set(idref,{"adata":{"f_"+datetostr:{"f_"+kind:ib["val"]}}},merge=True)
    if docref.exists:
        transaction.update(vrref,myattdata)
    else:
        transaction.set(vrref,myattdata)
    return dockey,None    
#if perstudentorkindflag =true,segreate per student
#if perstudentorkindflag =false,segregate per kind

def ndd():
        return defaultdict(ndd)

def SubmitAttendenceForMulti(transaction,db,offeringkey,sessionterm,mdate,kind,studentInfo,entitytype,entityid):
    datetostr=str(int(mdate))
    myattdata={"adata":{"f_"+kind:{"mdate":mdate,"kind":kind,"sti":studentInfo}}}
    for ib in studentInfo:
        #singleplacestore["att"][str(mdate)][kind][ib["vr"]][ib["id"]]=ib["val"]
        idref =db.collection(entitytype).document(entityid).collection("SESSIONTERM").document(sessionterm).collection("STUDATA").document(ib["id"]+"@att")
        transaction.set(idref,{"adata":{"f_"+datetostr:{"f_"+kind:ib["val"]}}},merge=True)
    dockey=datetostr+"@"+kind+"@att"
    transaction.set(db.collection(entitytype).document(entityid).collection("SESSIONTERM").document(sessionterm).collection("MULTI").document(dockey),myattdata,merge=True)
    return dockey,None

def SubmitProgressForVirtualRoom(transaction,db,virtualroomname,offeringkey,sessionterm,mdate,kind,studentInfo,totalscore,entitytype,entityid):
    dockey=kind
    myattdata={"adata":{"f_"+kind:{"mdate":mdate,"totalscore":totalscore,"kind":kind,"sti":studentInfo}}}
    docref =db.collection(entitytype).document(entityid).collection("SESSIONTERM").document(sessionterm).collection("VIRTUALROOMS").document(virtualroomname).collection("PROGRESS").document(dockey).get(transaction=transaction)
    vrref=db.collection(entitytype).document(entityid).collection("SESSIONTERM").document(sessionterm).collection("VIRTUALROOMS").document(virtualroomname).collection("PROGRESS").document(dockey)
    data={}
    ab = kind.split('_d_')
    kind1=None
    Kind2=None
    if len(ab)==1:
        kind1=kind
        kind2="un"
    elif len(ab)==2:
        kind1=ab[0]
        kind2=ab[1]
    else:
        kind1=kind
        kind2="un"
    for ib in studentInfo:
        data[ib["id"]]=ib["val"]
        mv = str(ib["val"]) if ib["val"] !=None else ""
        idref =db.collection(entitytype).document(entityid).collection("SESSIONTERM").document(sessionterm).collection("STUDATA").document(ib["id"]+"@pro")
        transaction.set(idref,{"adata":{"f_"+kind1:{"f_"+kind2:mv if totalscore ==None else mv + "/"+str(totalscore)}}},merge=True)
        #transaction.set(idref,{"adata":{"f_"+kind:mv if totalscore ==None else mv + "/"+str(totalscore)}},merge=True)
        
    if docref.exists:
        transaction.update(vrref,myattdata)
    else:
        transaction.set(vrref,myattdata)
    return dockey,None    

def SubmitProgressForMulti(transaction,db,offeringkey,sessionterm,mdate,kind,studentInfo,totalscore,entitytype,entityid):
    
    docrefdict ={}
    ab = kind.split("_d_")
    kind1=None
    Kind2=None
    if len(ab)==1:
        kind1=kind
        kind2="un"
    elif len(ab)==2:
        kind1=ab[0]
        kind2=ab[1]
    else:
        kind1=kind
        kind2="un"
    for ib in studentInfo:
        mv = str(ib["val"]) if ib["val"] !=None else ""
        idref =db.collection(entitytype).document(entityid).collection("SESSIONTERM").document(sessionterm).collection("STUDATA").document(ib["id"]+"@pro")
        transaction.set(idref,{"adata":{"f_"+kind1:{"f_"+kind2:mv if totalscore ==None else mv + "/"+str(totalscore)}}},merge=True)
        #transaction.set(idref,{"adata":{"f_"+kind:mv if totalscore ==None else mv + "/"+str(totalscore)}},merge=True)
    
    myattdata={"adata":{"f_"+kind:{"mdate":mdate,"totalscore":totalscore,"kind":kind,"sti":studentInfo}}}
    dockey=kind+"@pro"
    transaction.set(db.collection(entitytype).document(entityid).collection("SESSIONTERM").document(sessionterm).collection("MULTI").document(dockey),myattdata,merge=True)
    return dockey,None


def SubmitEventForVirtualRoom(transaction,db,virtualroomname,offeringkey,sessionterm,mdate,kind,evtdata,entitytype,entityid):
    datetostr=str(int(mdate))
    dockey=datetostr
    #docref =db.collection(entitytype).document(entityid).collection("SESSIONTERM").document(sessionterm).collection("VIRTUALROOMS").document(virtualroomname).collection("EVENT").document(dockey).get(transaction=transaction)
    evref=db.collection(entitytype).document(entityid).collection("SESSIONTERM").document(sessionterm).collection("VIRTUALROOMS").document(virtualroomname).collection("EVENT").document(dockey)
    myattdata={"adata":{"f_"+datetostr:{"f_"+kind:{"mdate":mdate,"kind":kind,"sti":evtdata}}}}
    transaction.set(evref,myattdata,merge=True)
    return dockey,None    

def SubmitEventForMulti(transaction,db,offeringkey,vrlist,sessionterm,mdate,kind,evtdata,entitytype,entityid):
    datetostr=str(int(mdate))
    dockey=datetostr+"@"+kind+"@evt"
    idkey =datetostr
    myattdata={"adata":{"f_"+datetostr:{"f_"+kind:{"mdate":mdate,"kind":kind,"sti":evtdata}}}}
    for s in  vrlist:
        vref=  db.collection(entitytype).document(entityid).collection("SESSIONTERM").document(sessionterm).collection("VIRTUALROOMS").document(s).collection("EVENT").document(idkey)
        transaction.set(vref,myattdata,merge=True)
    
    transaction.set(db.collection(entitytype).document(entityid).collection("SESSIONTERM").document(sessionterm).collection("MULTI").document(dockey),myattdata,merge=True)
    return dockey,None





def CreateOfferingSchedule(transaction,db,offeringschdata,entitytype,entityid):
    offeringname=offeringschdata["offeringname"]
    primaryowner = offeringschdata["primaryowner"]
    secondardaryowner= offeringschdata["secondaryowner"]
    sessionterm = offeringschdata["sessiontermname"]
    virtualroomname =offeringschdata["virtualroomname"]
    periodtype=offeringschdata["periodtype"]
    classperiodname=offeringschdata["classperiodname"]
    isindependentoffering =False
    if "isinde" in offeringschdata:
        isindependentoffering=offeringschdata["isinde"]
    offschkey= offeringname+"+_+"+periodtype+"+_+"+classperiodname
    profileid ="S_"+entityid
    channelid=None
    if virtualroomname ==None  :
        docpath =entitytype+'/'+entityid+'/SESSIONTERM/'+sessionterm+'/VIRTUALROOMS/'+virtualroomname
        print(docpath)
        doc1 = transaction.get(db.collection('SERVICEPROVIDERINFO').document(entityid).collection("SESSIONTERM").document(sessionterm).collection("VIRTUALROOMS").where(firestore1.field_path.FieldPath.document_id(), "==", db.document(docpath)).select(['channelid']))
        
        for x in doc1:
            if x.exists:
                m = x.to_dict()
                if "channelid" in m:
                    channelid = m["channelid"]
        if channelid is None:
            return None," Virtual Room is not defined properly"
    else:
        channelid = createchannel(db,primaryowner["display"]+":"+offeringname,entityid,"SERVICEPROVIDERINFO","ofr",["staff","manager"],["staff","manager"],transaction)
        offeringschdata["channelid_c"]=True
    if primaryowner is not None:
        OfferingScheduleChannelOperation(db,"add",primaryowner,channelid,profileid,None,True,transaction,entitytype,entityid,sessionterm,offschkey)         

    if secondardaryowner is not None:
        for ow in secondardaryowner:
            OfferingScheduleChannelOperation(db,"add",ow,channelid,profileid,None,False,transaction,entitytype,entityid,sessionterm,offschkey)             
    offeringschdata["channelid"]=channelid
    
    ofrref =db.collection(entitytype).document(entityid).collection("SESSIONTERM").document(sessionterm).collection("OFFERINGSCHEDULE").document(offschkey)
    transaction.set(ofrref,offeringschdata)
    return virtualroomname,None


def UpdateOfferingSchedule(transaction,db,channelid,offeringschkey,sessionterm,oldofferingschdata,newofferingschdata,entitytype,entityid):
    oldprimaryowner=None
    newprimaryowner=None
    oldsecondaryowner =None
    newsecondaryowner =None

    if 'primaryowner' in newofferingschdata:
        newprimaryowner = newofferingschdata["primaryowner"]
        return None,"Cannot change primary owner of offering schedule"


    if 'secondaryowner' in oldofferingschdata:
        oldsecondaryowner = oldofferingschdata["secondaryowner"]
    if 'secondaryowner' in newofferingschdata:
        newsecondaryowner = newofferingschdata["secondaryowner"]



    profileid ="S_"+entityid


    addlistsecondary=[]
    removelistsecondary=[]
    if oldsecondaryowner is  None and newsecondaryowner is not None:
        addlistsecondary=newsecondaryowner
    elif oldsecondaryowner is not  None and newsecondaryowner is  None:
        removelistsecondary=oldsecondaryowner
    elif oldsecondaryowner is not  None and newsecondaryowner is not  None:
        for userid in oldsecondaryowner:
            if userid not in newsecondaryowner:
                removelistsecondary.append(userid)

        for userid in newsecondaryowner:
            if userid not in oldsecondaryowner:
                addlistsecondary.append(userid)

    for userid in addlistsecondary:
        OfferingScheduleChannelOperation(db,"add",userid,channelid,profileid,None,False,transaction,entitytype,entityid,sessionterm,offeringschkey)         

    #OfferingScheduleChannelOperation(db,"add",ow,channelid,profileid,None,False,transaction,entitytype,entityid)
    for userid in removelistsecondary:
        OfferingScheduleChannelOperation(db,"remove",userid,channelid,profileid,None,False,transaction,entitytype,entityid,sessionterm,offeringschkey)         
    #print (removelistsecondary[0]["id"])
    #OfferingScheduleChannelOperation(db,"",removelistsecondary[0],channelid,profileid,None,False,transaction,entitytype,entityid)         

    recordref = db.collection(entitytype).document(entityid).collection("SESSIONTERM").document(sessionterm).collection("OFFERINGSCHEDULE").document(offeringschkey)
    transaction.update(recordref,newofferingschdata)
    return offeringschkey,None

def DeleteOfferingSchedule(transaction,db,offeringroomdata,entitytype,entityid):
    virtualroomname=offeringroomdata["virtualroomname"]
    primaryowner = offeringroomdata["primaryowner"]
    secondardaryowner= offeringroomdata["secondaryowner"]
    sessionterm = offeringroomdata["sessiontermname"]
    periodtype=offeringroomdata["periodtype"]
    classperiodname=offeringroomdata["classperiodname"]
    offschkey= primaryowner["id"]+"+_+"+periodtype+"+_+"+classperiodname
    profileid ="S_"+entityid

    channelid=offeringroomdata["channelid"]
    if primaryowner is not None:
        OfferingScheduleChannelOperation(db,"remove",primaryowner,channelid,profileid,None,True,transaction,entitytype,entityid,sessionterm,offschkey)         

    if secondardaryowner is not None:
        for ow in secondardaryowner:
            OfferingScheduleChannelOperation(db,"remove",ow,channelid,profileid,None,False,transaction,entitytype,entityid,sessionterm,offschkey)             

    transaction.delete(db.collection(entitytype).document(entityid).collection("SESSIONTERM").document(sessionterm).collection("OFFERINGSCHEDULE").document(offschkey))
    return virtualroomname,None


def CreateVirtualRoom(transaction,db,virtualroomdata,entitytype,entityid):
    virtualroomname=virtualroomdata["virtualroomname"]
    primaryowner = virtualroomdata["primaryowner"]
    secondardaryowner= virtualroomdata["secondaryowner"]
    sessionterm = virtualroomdata["sessiontermname"]
    profileid ="S_"+entityid
    channelid = createchannel(db,virtualroomname,entityid,"SERVICEPROVIDERINFO","vr",["staff","manager"],["staff","manager"],transaction)
    #set default value for the room
    
    
    if primaryowner is not None:
        VirtualRoomChannelOperation(db,"add",primaryowner,channelid,profileid,None,True,transaction,entitytype,entityid,sessionterm,virtualroomname)         

    if secondardaryowner is not None:
        for ow in secondardaryowner:
            VirtualRoomChannelOperation(db,"add",ow,channelid,profileid,None,False,transaction,entitytype,entityid,sessionterm,virtualroomname)             
    virtualroomdata["channelid"]=channelid
    transaction.set(db.collection(entitytype).document(entityid).collection("SESSIONTERM").document(sessionterm).collection("VIRTUALROOMS").document(virtualroomname),virtualroomdata)
    return virtualroomname,None

def UpdateVirtualRoom(transaction,db,channelid,virtualroomname,sessionterm,oldvirtualroomdata,newvirtualroomdata,entitytype,entityid):
    oldprimaryowner=None
    newprimaryowner=None
    oldsecondaryowner =None
    newsecondaryowner =None
    if 'primaryowner' in oldvirtualroomdata:
        oldprimaryowner = oldvirtualroomdata["primaryowner"]
    if 'primaryowner' in newvirtualroomdata:
        newprimaryowner = newvirtualroomdata["primaryowner"]

    if 'secondaryowner' in oldvirtualroomdata:
        oldsecondaryowner = oldvirtualroomdata["secondaryowner"]
    if 'secondaryowner' in newvirtualroomdata:
        newsecondaryowner = newvirtualroomdata["secondaryowner"]


    profileid ="S_"+entityid

    
    if oldprimaryowner is  None and newprimaryowner is not None:
        VirtualRoomChannelOperation(db,"add",newprimaryowner,channelid,profileid,None,True,transaction,entitytype,entityid,sessionterm,virtualroomname)         
    elif oldprimaryowner is not None and newprimaryowner is  None:
        VirtualRoomChannelOperation(db,"remove",oldprimaryowner,channelid,profileid,None,True,transaction,entitytype,entityid,sessionterm,virtualroomname)         
    elif oldprimaryowner is not None and newprimaryowner is not None and oldprimaryowner != newprimaryowner:
        VirtualRoomChannelOperation(db,"remove",oldprimaryowner,channelid,profileid,None,True,transaction,entitytype,entityid,sessionterm,virtualroomname)         
        VirtualRoomChannelOperation(db,"add",newprimaryowner,channelid,profileid,None,True,transaction,entitytype,entityid,sessionterm,virtualroomname)         

    addlistsecondary=[]
    removelistsecondary=[]
    if oldsecondaryowner is  None and newsecondaryowner is not None:
        addlistsecondary=newsecondaryowner
    elif oldsecondaryowner is not  None and newsecondaryowner is  None:
        removelistsecondary = oldsecondaryowner
    elif oldsecondaryowner is not  None and newsecondaryowner is not  None:
        for userid in oldsecondaryowner:
            if userid not in newsecondaryowner:
                removelistsecondary.append(userid)

        for userid in newsecondaryowner:
            if userid not in oldsecondaryowner:
                addlistsecondary.append(userid)

    for userid in addlistsecondary:
        VirtualRoomChannelOperation(db,"add",userid,channelid,profileid,None,False,transaction,entitytype,entityid,sessionterm,virtualroomname)         

    for userid in removelistsecondary:
        VirtualRoomChannelOperation(db,"remove",userid,channelid,profileid,None,False,transaction,entitytype,entityid,sessionterm,virtualroomname)         


    transaction.update(db.collection(entitytype).document(entityid).collection("SESSIONTERM").document(sessionterm).collection("VIRTUALROOMS").document(virtualroomname),newvirtualroomdata)
    return virtualroomname,None

def DeleteVirtualRoom(transaction,db,virtualroomdata,entitytype,entityid):
    virtualroomname=virtualroomdata["virtualroomname"]
    primaryowner = virtualroomdata["primaryowner"]
    secondardaryowner= virtualroomdata["secondaryowner"]
    sessionterm = virtualroomdata["sessiontermname"]
    profileid ="S_"+entityid

    channelid=virtualroomdata["channelid"]
    if primaryowner is not None:
        VirtualRoomChannelOperation(db,"remove",primaryowner,channelid,profileid,None,True,transaction,entitytype,entityid,sessionterm,virtualroomname)         

    if secondardaryowner is not None:
        for ow in secondardaryowner:
            VirtualRoomChannelOperation(db,"remove",ow,channelid,profileid,None,False,transaction,entitytype,entityid,sessionterm,virtualroomname)             

    transaction.delete(db.collection(entitytype).document(entityid).collection("SESSIONTERM").document(sessionterm).collection("VIRTUALROOMS").document(virtualroomname))
    return virtualroomname,None

# there should be only 1 primary member , 1 is must
def getsharedwithFromResidentUnitGroup(db,grpname,entitytype,entityid,transaction):
    doc12=transaction.get(db.collection(entitytype).document(entityid).collection("UNITGRPMEM").document(grpname).collection("DATA").where("appaccess",u"==",True).select(["appuserid","isprimary"]))
    primaryuserid =None
    sharedwith = []
    for x in doc12:
        mdoc1 =x
        mdoc =mdoc1.to_dict()
        isp =False
        if "isprimary" in  mdoc and mdoc["isprimary"] is not None:
            isp = mdoc["isprimary"]
        appuserid = None
        if "appuserid" in  mdoc and mdoc["appuserid"] is not None:
            appuserid = mdoc["appuserid"]
        
        if isp ==True:
            primaryuserid = appuserid
        else:
            sharedwith.append(appuserid)

        return primaryuserid,sharedwith
    return None,None

# there should be only 1 primary member , 1 is must
def getsharedwithFromDefaultOwnerGrp(db,grpname,entitytype,entityid,transaction):
    doc12 =transaction.get(db.collection(entitytype).document(entityid).collection("OWNERGRPMEM").document(grpname).collection("DATA").where("appaccess",u"==",True).select(["appuserid","isprimary"]))
    primaryuserid =None
    sharedwith = []
    for x in doc12:
        mdoc1 =x
        mdoc =mdoc1.to_dict()
        isp =False
        if "isprimary" in  mdoc and mdoc["isprimary"] is not None:
            isp = mdoc["isprimary"]
        appuserid = None
        if "appuserid" in  mdoc and mdoc["appuserid"] is not None:
            appuserid = mdoc["appuserid"]
        
        if isp ==True:
            primaryuserid = appuserid
        else:
            sharedwith.append(appuserid)

        return primaryuserid,sharedwith






def createupdateentrywithdatechange(matchedentry, enddate, othervalList,operation):
    mydata={}
    delflag =False
  
    if (operation == "insert"):
        mydata["relatedentry"]  = ArrayUnion([matchedentry])

    elif (operation == "update"):
        
        mydata = {}
        if matchedentry is not None:
            if("isc" in matchedentry and matchedentry["isc"] == True):
                mydata["enddate"] = enddate
            matchedentry["enddate"] = enddate
            othervalList.append(matchedentry)
        mydata["relatedentry"] = othervalList

    elif (operation == "delete"):
        mydata = {}
        if (len(othervalList) ==0):
            delflag=True
        else:
            if("isc" in matchedentry and matchedentry["isc"]==True):
                othervalList[0]["isc"]= True
                mydata["enddate"] = othervalList[0]["enddate"]
        
        mydata["relatedentry"] = othervalList
    
    return 	mydata,delflag
                    
    
def applyQRCodeChangesToSharedSubscription(db,transaction,changeddata,delflag,sharedwith,entitytype,entityid):
    if delflag==True:
        for userid in sharedwith:
            transaction.delete(db.collection(entitytype).document(entityid).collection("QRCODE").document(userid))
    else:
        for userid in sharedwith:
            transaction.update(db.collection(entitytype).document(entityid).collection("QRCODE").document(userid),changeddata)





# type - m for staff, r for resident , v - for vehicle, h -homehelp , g - guest/visitor,  
#Allowed channel type - internalnotification, internalgroupchat,externalnotification,
def handleqrcodeEntry(givenqrcode,name,photolink,appuserid,startdate,enddate,entitytype,entityid,mtype,unitaddress,servicereqid,existingqrcodeentry,db,transaction,operation,sharedwith):

    if(existingqrcodeentry is None and operation == "insert"):
        mydata = complex_qrcodeentryServiceReq(givenqrcode,name,photolink,appuserid,startdate,enddate,entitytype,mtype,unitaddress,servicereqid)
        transaction.set(db.collection(entitytype).document(entityid).collection("QRCODE").document(givenqrcode),mydata)
    else:
        relatedentry = None
        mydata =None
        delflag =False
        if  existingqrcodeentry is not None and "relatedentry" in  existingqrcodeentry:
            relatedentry =  existingqrcodeentry["relatedentry"]

        #breakpoint()
        matchedVal,othervalList,qrcodecurrent_entry,qrcodecurrent_staff = findmatchedentry(unitaddress,servicereqid,relatedentry,mtype)
        #only 1 value is required for staff - its a big issue - one shouldnt insert the same staff again
        if (operation == "insert"  ):
            if matchedVal is  None :
                matchedVal={"mtype":mtype,"startdate":startdate,"enddate":enddate,"isc":False}
                if (unitaddress is not None):
                    matchedVal["rd"] =unitaddress
                if (servicereqid is not None):
                    matchedVal["sr"] =servicereqid


            mydata,delflag = createupdateentrywithdatechange(matchedVal, enddate, othervalList,"insert")

        elif (operation == "update"):
            
            print(relatedentry)
            if relatedentry is None:
                
                transaction.update(db.collection(entitytype).document(entityid).collection("QRCODE").document(givenqrcode),{"enddate":enddate})
                return
            
            if matchedVal is  None :
                matchedVal={"mtype":mtype,"startdate":startdate,"enddate":enddate,"isc":False}
                if (unitaddress is not None):
                    matchedVal["rd"] =unitaddress
                if (servicereqid is not None):
                    matchedVal["sr"] =servicereqid

            mydata,delflag = createupdateentrywithdatechange(matchedVal, enddate, othervalList,"update")
        elif (operation == "delete"):
            mydata,delflag = createupdateentrywithdatechange(matchedVal, enddate, othervalList,"delete")
        #breakpoint()
        if delflag :
            transaction.delete(db.collection(entitytype).document(entityid).collection("QRCODE").document(givenqrcode))		
        else:
            transaction.update(db.collection(entitytype).document(entityid).collection("QRCODE").document(givenqrcode),mydata)

        if mtype == "r" and sharedwith is not None:
            applyQRCodeChangesToSharedSubscription(db,transaction,mydata,delflag,sharedwith,entitytype,entityid)		    		

def createchannel(db,channelname,channelowner,channelownertype,channeltype,readarraymem,writearraymem,batch):

##create empty document in CROOM
    mydata={}
    chat_ref = db.collection(u'CROOM').document()
            
    batch.set(chat_ref,mydata)

##create  document in CROOMMETA
    
    mydata={}
    mydata["version"] =1
    mydata["creattime"] =datetime.now()
    mydata["roomname"] =channelname
    mydata["id"] =chat_ref.id              
    mydata["roomtype"] =channeltype
    mydata["roomownertype"] =channelownertype
    mydata["roomownerid"] =channelowner
    mydata["roomTo"] =None
    mydata["isactive"] =True
    mydata["markfordeletion"] =False
    mydata["readusers"] =readarraymem
    mydata["writeusers"] =writearraymem

    chat_ref1 = db.collection(u'CROOMMETA').document()
    batch.set(chat_ref1, mydata)
    print (chat_ref1.id)
    print (mydata)
    return 	chat_ref1.id


def addEntityToUserProfile(db,operationtype,userid, readchannel ,ownerunits, residentunits,isemployee,entitytype,entityid,ownertenatchannel,roles,channelsupplyer,channelcommunicate,isactive,profileid,setNoneList,vehqrcode, channels_virtualroom,batch,sharedwith):
    id =None
    myaddionalinfo ={}
    entityreg =False
    if "_R_" in profileid:
        entityreg=True

    if(operationtype=="insert") :
        mydata={}
        mydata["isactive"] =isactive
        mydata["isemployee"] =isemployee
        mydata["roles"] =roles
        mydata["channels"] =readchannel
        mydata["ownerunits"] =ownerunits
        mydata["residentunits"] =residentunits
        ##converted to arrary in addresident details
        if ownertenatchannel is not None:
            mydata["channels_oc"] =ownertenatchannel
        if channelcommunicate is not None:
            mydata["channels_communicate"] =channelcommunicate
        if channelsupplyer is not None:
            mydata["channels_supplyer"] =channelsupplyer
        if vehqrcode is not None:
            mydata["vehqrcode"] =vehqrcode

            

        user_ref = db.collection(u'USERS').document(userid)
        batch.update(user_ref,{profileid:mydata})
        entityuser_ref = db.collection(entitytype).document(entityid).collection(u"internaldata").document("first")
        
        if entityreg:
            batch.update(entityuser_ref,{"rusers":ArrayUnion([userid])})
        else:	
            batch.update(entityuser_ref,{"users":ArrayUnion([userid])})
        

    elif operationtype == "update":
        if setNoneList is  None:
            setNoneList =[]
        mydata ={}
        initialpathstring =profileid + "."  # we are using dot notation as we are updating nested value
        if(isactive is not None or "isactive" in setNoneList):
            mydata[initialpathstring + "isactive"] =isactive
        if(  isemployee is not None or "isemployee" in setNoneList):
            mydata[initialpathstring + "isemployee"] =isemployee
        if(  roles is not None or "roles" in setNoneList):
            mydata[initialpathstring + "roles"] =roles

        if(  readchannel is not None or "channels" in setNoneList):
            mydata[initialpathstring + "channels"] =readchannel
        if("ownerunits" in setNoneList or ownerunits is not None):
            mydata[initialpathstring + "ownerunits"] =ownerunits
        if("residentunits" in setNoneList or residentunits is not None):
            mydata[initialpathstring + "residentunits"] =residentunits
        if("channels_oc" in setNoneList or ownertenatchannel is not None):
            mydata[initialpathstring + "channels_oc"] =ownertenatchannel

        if("channels_vr" in setNoneList or channels_virtualroom is not None):
            mydata[initialpathstring + "channels_vr"] =channels_virtualroom
        if("channels_communicate" in setNoneList or channelcommunicate is not None):
            mydata[initialpathstring + "channels_communicate"] =channelcommunicate
        if("channels_supplyer" in setNoneList or channelsupplyer is not None):
            mydata[initialpathstring + "channels_supplyer"] =channelsupplyer
        if("channels_supplyer" in setNoneList or channelsupplyer is not None):
            mydata[initialpathstring + "channels_supplyer"] =channelsupplyer
        if("vehicleqrcode" in setNoneList or vehqrcode is not None):
            mydata[initialpathstring + "vehicleqrcode"] =channelsupplyer

        #user_ref = db.collection(u'USERS').document(userid)
        #batch.update(user_ref,mydata)

        updateUserProfileWithSharedProfileConsideration(db,userid,sharedwith,mydata,batch,entityreg,operationtype,entitytype,entityid,profileid)			
    elif operationtype == "remove":
        updateUserProfileWithSharedProfileConsideration(db,userid,sharedwith,None,batch,entityreg,operationtype,entitytype,entityid,profileid)


def complex_registryentryOwner (name, unitaddr,appuserid,publishedcontact,management,startdate,enddate,opcf):
    mydata={}
    mydata["version"] =1
    mydata["unitaddress"] =unitaddr
    mydata["ownername"] =name
    mydata["ownergroup"] =None
    mydata["owneruserid"] =appuserid
    mydata["ownertoken"] = "abcdbd"
    mydata["ownerrecvmsg"] = False
    mydata["ownerpublishedcontact"] =publishedcontact
    mydata["ownermanagementposition"] =management
    mydata["ownerstartdate"] =startdate
    mydata["ownerenddate"] =enddate
    mydata["opcf"] =opcf
    mydata["serversidetimestamp"] =firestore.SERVER_TIMESTAMP
    return mydata
 
def complex_registryentryOwnerEmptyIt ():
    mydata={}
    mydata["version"] =1
    mydata["unitaddress"] =None
    mydata["ownername"] =None
    mydata["ownergroup"] =None
    mydata["owneruserid"] =None
    mydata["ownertoken"] = None
    mydata["ownerrecvmsg"] = None
    mydata["ownerpublishedcontact"] =None
    mydata["ownermanagementposition"] =None
    mydata["ownerstartdate"] =None
    mydata["ownerenddate"] =None
    mydata["ownerpublishedcontactflag"] =None
    mydata["serversidetimestamp"] =firestore.SERVER_TIMESTAMP
    return mydata



def complex_registryentryResident (name, unitaddr,appuserid,publishedcontact,management,startdate,enddate,rpcf):
    mydata={}
    mydata["version"] =1
    mydata["unitaddress"] =unitaddr
    mydata["residentname"] =name
    mydata["residentuserid"] =appuserid
    mydata["residentrecvmsg"] =False
    mydata["residenttoken"] = "abcbcbc"
    mydata["residentpublishedcontact"] =publishedcontact
    mydata["residentmanagementposition"] =management
    mydata["residentstartdate"] =startdate
    mydata["residentenddate"] =enddate
    mydata["residentpublishedcontactflag"]=rpcf

    mydata["serversidetimestamp"] =firestore.SERVER_TIMESTAMP 

    return mydata

def complex_registryentryResidentEmptyIt ():
    mydata={}
    mydata["version"] =1
    mydata["unitaddress"] =None
    mydata["residentname"] =None
    mydata["residentuserid"] =None
    mydata["residentrecvmsg"] =None
    mydata["residenttoken"] = None
    mydata["residentpublishedcontact"] =None
    mydata["residentmanagementposition"] =None
    mydata["residentstartdate"] =None
    mydata["residentenddate"] =None
    mydata["residentpublishedcontactflag"]=None

    mydata["serversidetimestamp"] =firestore.SERVER_TIMESTAMP 

    return mydata



def complex_registryentryResidentOwnerGrp (name, ownergroup,unitaddr,appuserid,publishedcontact,management,pcf):
    mydata={}
    mydata["version"] =1
    mydata["unitaddress"] =unitaddr
    mydata["ownergroup"] =ownergroup
    mydata["owneruserid"] =None
    mydata["ownertoken"] = None
    mydata["ownerrecvmsg"] = None
    mydata["ownerpublishedcontact"] =None
    mydata["residentname"] =name
    mydata["residentuserid"] =appuserid
    mydata["residenttoken"] = "abcdbd"
    mydata["residentpublishedcontact"] =publishedcontact
    mydata["ismanagement"] =management
    mydata["residentpublishedcontactflag"]=pcf
    mydata["residentrecvmsg"] = False
    mydata["serversidetimestamp"] =firestore.SERVER_TIMESTAMP              
    return mydata


def generateqrcodeandsave(storage,pathwithfilename,qrcodestr):
    bucket = storage.bucket()
    qr = qrcode.QRCode(version=1,error_correction=qrcode.constants.ERROR_CORRECT_L,box_size=10, border=4,)
    qr.add_data(qrcodestr)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    imgByteArr = io.BytesIO()
    img.save(imgByteArr, format='PNG')
    imgByteArr = imgByteArr.getvalue()
    file = bucket.blob(pathwithfilename)
    file.upload_from_string(imgByteArr)
    return file.public_url





def RoleBasedChannelListFromEntity(db,role,entitytype,entityid,ntransaction,alreadyReadFieldDoc):
    myfieldsrequired =[]
    myfieldsrequired.append("channels")
    channels = []
    channels_communicate = []
    channels_supplyer = []
    setchannels =[]
    if ("COMMUNICATE" in role):
        myfieldsrequired.append("channels_communicate")
        setchannels.append("channels_communicate")
        #myfieldsrequired=myfieldsrequired + "," + "channels_communicate"

    if ("SUPPLYER" in role):
        myfieldsrequired.append("channels_supplyer")
        setchannels.append("channels_supplyer")
        #myfieldsrequired=myfieldsrequired + "," + "channels_supplyer"

    #doc = db.collection(entitytype).document(entityid).get(myfieldsrequired,transaction=ntransaction)
    if alreadyReadFieldDoc is  None:

        doc = db.collection(entitytype).document(entityid).get(transaction=ntransaction)
        mdoc=doc.to_dict()
    else:
        mdoc = alreadyReadFieldDoc
    print(mdoc)
    print (role)
    if("channels_communicate" in  mdoc):
        channels_communicate = 	mdoc["channels_communicate"]
    else:
        channels_communicate=None

    if("channels_supplyer" in  mdoc):
        channels_supplyer = 	mdoc["channels_supplyer"]
    else:
        channels_communicate=None
		

    returnedchannel =mdoc["channels"]
    for item in returnedchannel:
        doc12 = db.collection("CROOMMETA").document(item).get()
        mdoc1 =doc12.to_dict()
        #print("i am doc1")
        #print(mdoc1)

        print (mdoc1["readusers"])
        print(role)
        rights=""
        if (role is not None  and mdoc1["readusers"] is not None):
            if (len(list(set(role) & set(mdoc1["readusers"]))) >0):  #intersection of 2 arrays
                print("found read match")
                rights="r"
        else:
            print("found no read match")
    
        print (mdoc1["writeusers"] )
        print(role)
        if (role is not None  and mdoc1["writeusers"] is not None):
            if (len(list(set(role) & set(mdoc1["writeusers"]))) >0): #intersection of 2 arrays
                print("found write match")
                rights=rights + "w"
        else:
            print("found no write match")

        if (len(rights) > 0):
            channels.append({"channel":item,"rights":rights})
			
    
    return 	channels,channels_communicate,channels_supplyer,setchannels,mdoc
	
def archivedata(data,entitytype,entityid,byuserid, collectiontype, collectionkey,transaction,db):
    mydata ={}
    mydata["version"]=1
    mydata["data"]=data
    mydata["collectiontype"]=collectiontype
    mydata["collectionkey"]=collectionkey
    mydata["serversidetimestamp"] =firestore.SERVER_TIMESTAMP
    mydata["byuserid"] =byuserid
    transaction.set(db.collection(entitytype).document(entityid).collection(u'ARCHIVE').document(),mydata)


def doEndDateCheck(masterdate,givendate):
    #print (masterdate + "   " + givendate)
    if masterdate is None:
        return True
    else:
        if givendate is None:
            return False
        else:
            if masterdate >= givendate:
                print ("i am at ttue")
                return True
            else:
                print (masterdate >= givendate)
                print ("i am at false")
                return False
			
    
##Need to check first for resident , then for owner (we can owner living in the apartment , in that case there will be no _r entry
def checkResidentDetailsEndDate(residentdetailsid,givendate,isowner,entitytype,entityid,db):


    
    val=None
    myref = db.collection(entitytype).document(entityid).collection(u'RESIDENTDETAILS').document(residentdetailsid).get(["enddate"])
    if myref.exists:
        mdoc =myref.to_dict()
        val = mdoc["enddate"]
    else:
        val =None
    
    return doEndDateCheck(val,givendate)
	

def checkStaffMemberEndDate(appuserid,givendate,entitytype,entityid,db):
    val=None
    myref = db.collection(entitytype).document(entityid).collection(u'STAFF').document(appuserid).get(["enddate"])
    if myref.exists:
        mdoc =myref.to_dict()
        val = mdoc["enddate"]
    else:
        val =None
    
    return doEndDateCheck(val,givendate)
	

def checkShiftPlanRequestEndDate(db,entitytype,entityid,enddatevalue,foruserid):

    shiftq =db.collection(entitytype).document(entityid).collection(u'SHIFTPLANS').where('employees',u'array_contains',foruserid).select(["enddate"]).stream() #.get(["enddate"])
    countshiftq=0
    for h in shiftq:
        if h.exists:
            docdict =h.to_dict()
            print (docdict)
            if("enddate" in docdict ):
                if docdict["enddate"] is None:
                    countshiftq=countshiftq+1
                else:
                    if  docdict["enddate"] > enddatevalue:
                        countshiftq=countshiftq+1
    return countshiftq

def checkVehicleAndServiceRequestEndDate(db,entitytype,entityid,enddatevalue,foruserid):

    vehq =db.collection(entitytype).document(entityid).collection(u'COMPLEXVEHICLEREG').where('appuserid',u'==',foruserid).select(["enddate"]).stream() #.get(["enddate"])
    countvehq=0
    for h in vehq:
        if h.exists:
            docdict =h.to_dict()
            print (docdict)
            if("enddate" in docdict ):
                if docdict["enddate"] is None:
                    countvehq=countvehq+1
                else:
                    if  docdict["enddate"] > enddatevalue:
                        countvehq=countvehq+1

	    
    servicereq =db.collection(entitytype).document(entityid).collection(u'SERVICEREQUESTS').where('requesterId',u'==',foruserid).select(["enddate"]).stream()
    countserq=0
    for h in servicereq:
        if h.exists:
            docdict =h.to_dict()
            print (docdict)
            if("enddate" in docdict ):
                if docdict["enddate"] is None:
                    countserq=countserq+1
                else:
                    if  docdict["enddate"] > enddatevalue:
                        countserq=countserq+1

    return countvehq,countserq



    



def AddResidentDetails( transaction,db, residentdetailsid,residentdata,residentdeltadata,entitytype,entityid,profileid,storage,providedregisteras,providedresidentid,providedunitaddress,providedresidentunit,doregistryworkflag):
    registeras = None
    residentid =None
    unitaddress = None
    concerneduserid = None
    feescheduleid = None
    fromManagementflag = None
    publishedcontact = None
    ownergroup = None
    doownerprocessingfortenantchannelifrequired =True
    doregistryread = True
    displayname =None
    isManagement = False
    startdate = None
    enddate=None
    publishcontactflag=None
    if residentdata is not None:
        registeras = residentdata["registeras"]
        unitaddress = residentdata["unitaddress"]
        residentid = residentdata["appuserid"]
        fromManagementflag = residentdata["fromManagementflag"]
        publishedcontact = residentdata["publishedcontact"]
        startdate = residentdata["startdate"]
        enddate = residentdata["enddate"]
        if "publishedcontactflag" in residentdata:
            publishcontactflag=residentdata["publishedcontactflag"]
        
        if residentdata["firstname"] is not None and residentdata["lastname"] is not None:
            displayname = residentdata["lastname"] +"," + residentdata["firstname"]
        mp = residentdata["managementposition"]
        if mp is not None and len(mp) >0: 
            isManagement = True
        print(residentid)
        print(fromManagementflag)
        if residentid is None and (fromManagementflag is None or fromManagementflag ==False):
            return None, "Provided user doesnt have appuser" ##Should log in critical error section
        else:
            firstname = residentdata["firstname"]
            lastname = residentdata["lastname"]
            email = residentdata["email"]
            contactnumber = residentdata["contactnumber"]
            if residentid is None:
                residentid = AddNewAppUser(db,storage,lastname+"," + firstname, email, contactnumber,False)
                residentdata["appuserid"] =residentid
        if fromManagementflag is not None and fromManagementflag == True:
            ownergroup = residentdata["ownergroup"]

    else:
        registeras = providedregisteras
        residentid = providedresidentid
        unitaddress = providedunitaddress
        doownerprocessingfortenantchannelifrequired =False
        doregistryread = False

    if registeras =="owner" or registeras =="OWNER":
        registeras="owner"
        if residentdata is not None:
            residentdata["registeras"]="owner"
    if registeras =="resident" or registeras =="RESIDENT":
        registeras="resident"
        if residentdata is not None:
            residentdata["registeras"]="resident"
    
    readfortenantflag= True
    oc =None #ownertenantchannel
    if registeras =="owner" or registeras =="OWNER":
        readfortenantflag= False
        doownerprocessingfortenantchannelifrequired =False


    operationtype,name,qrcode,photolink, existingentry,matchedresidentunit,residentunitswithoutunitaddress,channels_ownertennantwithoutunitaddress,ownertenantchannel,owneruserid,residentuserid,qrcodesendnotificationwithoutunitaddress,vehqrcode_complete,ownergroup,qrcodetotal_entry,registrydata,sharedwith = ProcessUserRecord(db,residentdetailsid, residentid, readfortenantflag,True,entitytype,entityid,True,None, None,transaction,doregistryread)
    if registeras == "owner" and owneruserid is not None:
        return None,"Unit already has a registered owner"
    if registeras == "resident" and residentuserid is not None:
        return None,"Unit already has a registered resident"

    oc=None
    if channels_ownertennantwithoutunitaddress is None:
        if ownertenantchannel is not None:
            oc =[ownertenantchannel]
    else:
        if ownertenantchannel is not None:
            channels_ownertennantwithoutunitaddress.append(ownertenantchannel)
            oc =channels_ownertennantwithoutunitaddress
        else:
            oc =channels_ownertennantwithoutunitaddress

    finalresidentunits = []
    if providedresidentunit is None:
        if matchedresidentunit is  None:
            matchedresidentunit = {"rd":residentdetailsid,"m" :isManagement,"sw":[]}
    else:
        matchedresidentunit = providedresidentunit
    finalresidentunits.append(matchedresidentunit)

    if  (residentunitswithoutunitaddress is not None and len(residentunitswithoutunitaddress)  > 0 ): 
        finalresidentunits.extend(residentunitswithoutunitaddress)
    
    newroles = getRoleAndChannelForResidentBasedonResidentUnit(finalresidentunits)
    channels_entity,channels_communicate,channels_supplyer,updatelist,chdata=RoleBasedChannelListFromEntity(db,newroles,entitytype,entityid,transaction,None)
    qrcodetype = "r"
     
    #as we need to finish read before write , we do the read for ownershared with now, and update it later
    ownersharewith = None
    if doownerprocessingfortenantchannelifrequired == True and registeras == "resident" and residentuserid != owneruserid:
        if owneruserid is None and ownergroup is not None:
            pu,ownersharewith=getSharedWithForResidentDetails(db,ownergroup,entitytype,entityid,transaction)
        if owneruserid is not None :
            pu,ownersharewith=getsharedwithFromResidentUnitGroup(db,unitaddress+"_o",entitytype,entityid,transaction)
        
    handleqrcodeEntry(qrcode,displayname,None,residentid,startdate,enddate,entitytype,entityid,qrcodetype,residentdetailsid,None,qrcodetotal_entry,db,transaction,operationtype,None)

    addEntityToUserProfile(db,operationtype,residentid, channels_entity ,None,finalresidentunits,None,entitytype,entityid,oc,newroles,channels_supplyer,channels_communicate,None,profileid,None,None, None,transaction,None)

    if doownerprocessingfortenantchannelifrequired == True and registeras == "resident" and residentuserid != owneruserid:
        OwnerTenantChannelOperation(db,owneruserid,ownersharewith,ownertenantchannel,profileid,unitaddress,"add",transaction,entitytype,entityid)

    if doregistryworkflag == True:
        myregistryentry={}
        if  residentdata["registeras"] == "owner" or residentdata["registeras"] =="OWNER":
            myregistryentry = complex_registryentryOwner (displayname , residentdata["unitaddress"],residentdata["appuserid"],residentdata["publishedcontact"],residentdata["managementposition"],residentdata["startdate"],residentdata["enddate"],publishcontactflag)
            transaction.set(db.collection("COMPLEXES").document(entityid).collection("REGISTRY").document(unitaddress),myregistryentry)
        else:
            if ownergroup is None:
                myregistryentry = complex_registryentryResident (displayname , residentdata["unitaddress"],residentdata["appuserid"],residentdata["publishedcontact"],residentdata["managementposition"],residentdata["startdate"],residentdata["enddate"],publishcontactflag)
            else:
                myregistryentry = complex_registryentryResidentOwnerGrp (displayname,ownergroup , residentdata["unitaddress"],residentdata["appuserid"],residentdata["publishedcontact"],residentdata["managementposition"],publishedcontact)

            if  owneruserid==None and residentuserid == None:
                transaction.set(db.collection("COMPLEXES").document(entityid).collection("REGISTRY").document(unitaddress),myregistryentry)
            else:
                transaction.update(db.collection("COMPLEXES").document(entityid).collection("REGISTRY").document(unitaddress),myregistryentry)

        #create entry in ResidentUnitGroup
    if residentdata is not None:
        mydata12={}
        mydata12["version"] =1
        mydata12["appuserid"] =residentid
        mydata12["name"] = displayname
        mydata12["phonenumber"] = residentdata["contactnumber"]
        mydata12["email"] =residentdata["email"]
        mydata12["relation"] =None
        mydata12["appaccess"] = True
        mydata12["isprimary"] = True
        mydata12["grpname"] =residentdetailsid
        transaction.set(db.collection(entitytype).document(entityid).collection("UNITGRPMEM").document(residentdetailsid).collection("DATA").document(residentid),mydata12)
        mydata2345={}
        mydata2345["fm"]=[residentid]
        transaction.set(db.collection(entitytype).document(entityid).collection("UNITGRPMEM").document(residentdetailsid),mydata2345)

    return unitaddress,None

def ShareSubscriptionForGroup(db,actiontype,grpname,memberidlistold,memberidlistnew,unitaddresslistold,unitaddresslistnew,entitytype,entityid,transaction):
    entitykey = "C_"+ entityid
    print (memberidlistold)
    print (memberidlistnew)
    print (unitaddresslistold)
    print (unitaddresslistnew)
    return grpname,None
"""
    if actiontype == "share" :
        userprofiledata=None
        doc12 = transaction.get(db.collection("USERS").document(memberid))
        mdoc12 =None
        for x in doc12:
            mdoc12 =x
            if mdoc12 is not None:
                userprofiledata =mdoc12.to_dict()
                
                if entitykey in  userprofiledata:
                    entityspecificdata =  userprofiledata["entitykey"]
                    if entityspecificdata is not None and "channels_oc" in entityspecificdata and entityspecificdata["channels_oc"] is not None:
                        transaction.update(db.collection("USERS").document(memberid),{entitykey+"."+"channels_oc":entityspecificdata["channels_oc"]})

        transaction.update(db.collection(entitytype).document(entityid).collection("OWNERGRPMEM").document(grpname).collection("DATA").document(memberid),{"appaccess":True})   
        return  memberid,None                              
    elif actiontype == "unshare":
        transaction.update(db.collection("USERS").document(memberid),{entitykey +"."+"channels_oc":None})
        transaction.update(db.collection(entitytype).document(entityid).collection("OWNERGRPMEM").document(grpname).collection("DATA").document(memberid),{"appaccess":False})   
        return  memberid,None                            
"""
    

def ShareSubscriptionForResidentUnit(transaction,db,actiontype,sharewithuserid,residentdetailsid,entitytype,entityid):
    providedregisteras="owner"
    providedunitaddress=None
    if residentdetailsid.endswith("_r"):
        providedregisteras="resident"
        providedunitaddress =residentdetailsid.replace("_r","")
    else:
        providedunitaddress =residentdetailsid.replace("_o","")
    profileid ="C_R_"+ entityid
    if actiontype == "share" :

        
        residentunit = {"rd":residentdetailsid,"m" :False,"sw":[]}

        a,b = AddResidentDetails(transaction,db , residentdetailsid,None,None,entitytype,entityid,profileid,storage,providedregisteras,sharewithuserid,providedunitaddress,residentunit,False)
        return a,b

    elif actiontype == "unshare" :
        a=12
        unshareUnitSubscription(db,residentdetailsid,sharewithuserid,entitytype,entityid,transaction,profileid)
        return sharewithuserid,None
        #DeleteResidentDetailsEntry(db,residentdetailsid,sharewithuserid,entitytype,entityid,providedunitaddress,None,transaction,True)




    



#if unitaddress is none , that means vehicle is for staff member

def UpdateServiceRequest(transaction,db,givenqrcode,requesttype,olddata,newdata,appuserid,byuserid,unitaddress,servicereqid,entitytype,entityid):
    id =None
    rejectstring=None
    qrcodetype =None
    if 	requesttype ==  "HOMEHELP":
        qrcodetype="h"
    elif requesttype ==  "VISITOR":
        qrcodetype="g"
    print ("in update service req")   
    if "terminateflag" in newdata:
	
        qrcodeentry=None
        if givenqrcode is not None and requesttype == "HOMEHELP":
            doc12 = db.collection(entitytype).document(entityid).collection(u'QRCODE').document(givenqrcode).get(transaction=transaction)
            if mdoc12 is not None:
                qrcodeentry =mdoc12.to_dict()            			
        doc12 = db.collection(entitytype).document(entityid).collection(u'SERVICEREQUESTS').document(servicereqid).get(transaction=transaction)
        if doc12.exists:
            mdoc =mdoc1.to_dict()
            archivedata(mdoc,entitytype,entityid,byuserid, u'SERVICEREQUESTS', servicereqid,transaction,db)
            if requesttype ==  "HOMEHELP" or   requesttype ==  "VISITOR" :
                handleqrcodeEntry(givenqrcode,None,None,None,None,None,entitytype,entityid,qrcodetype,None,servicereqid,qrcodeentry,db,transaction,"delete",None)
            transaction.delete(db.collection(entitytype).document(entityid).collection(u'SERVICEREQUESTS').document(servicereqid))        		
        return  servicereqid,None          
		
    elif "enddate" in newdata :
        givendate = newdata["enddate"]
        checkresult = True
        if (unitaddress is not None):
            checkresult = checkResidentDetailsEndDate(unitaddress,givendate,False,entitytype,entityid,db)
        else:
            checkresult = checkStaffMemberEndDate(appuserid,givendate,entitytype,entityid,db)
         	
        if checkresult==False:
            return id, "Provided End Date is greater than tenure of entity"
        
        qrcodeentry=None
        print (requesttype)
        print (givenqrcode)        
        if givenqrcode is not None and requesttype == "HOMEHELP":
            doc123 = transaction.get(db.collection(entitytype).document(entityid).collection(u'QRCODE').document(givenqrcode))
            mdoc12 =None
            for x in doc123:
                mdoc12 =x
            if mdoc12 is not None:
                qrcodeentry =mdoc12.to_dict()            			
        print ("in update service")
        print (qrcodeentry)
        if requesttype ==  "HOMEHELP" or   requesttype ==  "VISITOR" :
            handleqrcodeEntry(givenqrcode,None,None,None,None,givendate,entitytype,entityid,qrcodetype,None,servicereqid,qrcodeentry,db,transaction,"update",None)
        transaction.update(db.collection(entitytype).document(entityid).collection(u'SERVICEREQUESTS').document(servicereqid),newdata)
        return servicereqid,None
    
    else:
        transaction.update(db.collection(entitytype).document(entityid).collection(u'SERVICEREQUESTS').document(servicereqid),newdata)		
        return servicereqid,None


#if unitaddress is none , that means vehicle is for staff member
def UpdateVehicleRequest(transaction,db,isvisitor,olddata,newdata,appuserid,byuserid,unitaddress,numberplate,entitytype,entityid):
    id =None
    rejectstring=None
   
    if "terminateflag" in newdata and isvisitor ==False:
        entitychar ="C_"
        if entitytype== "SERVICEPROVIDERINFO":
            entitychar ="S_"
        if unitaddress is not None:
            entitychar=entitychar+"R_"
        doc12 = transaction.get(db.collection(entitytype).document(entityid).collection(u'COMPLEXVEHICLEREG').document(numberplate))
        mdoc1 =None
        for x in doc12:
            mdoc1 =x
        if mdoc1 is not None:
            mdoc =mdoc1.to_dict()
            archivedata(mdoc,entitytype,entityid,byuserid, u'COMPLEXVEHICLEREG', numberplate,transaction,db)
            handleqrcodeEntry(numberplate,None,None,None,None,None,entitytype,entityid,type,None,None,None,db,transaction,"delete",None)
            transaction.delete(db.collection(entitytype).document(entityid).collection(u'COMPLEXVEHICLEREG').document(numberplate))        		
            return numberplate,None
        return numberplate,"NO VEHICLE DEFINED"
    elif "enddate" in newdata and isvisitor ==False:
        givendate = newdata["enddate"]
        checkresult = True
        if (unitaddress is not None):
            checkresult = checkResidentDetailsEndDate(unitaddress,givendate,False,entitytype,entityid,db)
        else:
            checkresult = checkStaffMemberEndDate(appuserid,givendate,entitytype,entityid,db)
         	
        if checkresult==False:
            return id, "Provided End Date is greater than tenure of entity"
        
		
        handleqrcodeEntry(numberplate,None,None,None,None,givendate,entitytype,entityid,type,None,None,None,db,transaction,"update",None)
        transaction.update(db.collection(entitytype).document(entityid).collection(u'COMPLEXVEHICLEREG').document(numberplate),newdata)
        return numberplate,None
    
    else:
        if "terminateflag" in newdata :
            entitychar ="C_"
            if entitytype== "SERVICEPROVIDERINFO":
                entitychar ="S_"
            if unitaddress is not None:
                entitychar=entitychar+"R_"
            doc12 = db.collection(entitytype).document(entityid).collection(u'COMPLEXVEHICLEREG').document(numberplate).get(transaction=transaction)
            if doc12.exists :
                mdoc =doc12.to_dict()
                archivedata(mdoc,entitytype,entityid,byuserid, u'COMPLEXVEHICLEREG', numberplate,transaction,db)
                transaction.delete(db.collection(entitytype).document(entityid).collection(u'COMPLEXVEHICLEREG').document(numberplate))        		
                return numberplate,None
            return numberplate,"NO VEHICLE DEFINED"

        transaction.update(db.collection(entitytype).document(entityid).collection(u'COMPLEXVEHICLEREG').document(numberplate),newdata)		
        return numberplate,None
    
def getSharedWithForResidentDetails(db,residetailsid,appuserid, profilefieldname,transaction):
    sharedwith=None
    doc12 = transaction.get(db.collection("USERS").document(appuserid))
    mdoc1 =None
    for x in doc12:
        mdoc1 =x
    mdoc =mdoc1.to_dict()
    print(mdoc)
    name =mdoc["name"]
    qrcode =mdoc["qrcode"]
    if profilefieldname in mdoc:
        existingentry = mdoc[profilefieldname]
        if "residentunits" in 	existingentry  and existingentry["residentunits"] is not None:
            for ru in existingentry["residentunits"]:
                if ru["rd"] == residetailsid:
                    if "sw" in ru:
                        sharedwith=  ru["sw"]
    return sharedwith


def getunitaddressfromresidentdetails(rd):
    if rd is not None:
        ab =rd.replace("_r","").replace("_o","")
        return ab
    return None

#channels_oc - owner-tenant channel -- can be multiple --only relevant if reqcheck =True

#channels_cn  -- channel for notification  --- if user has subscibed for notification -- --only relevant if reqcheck =True  -- is always going to be one , can be set or unset - so no need processing . Not relevant in case of Create/Update/Delete
#channels_o2o  -- one on one channel ----only relevant if reqcheck =True -- is always going to be 1  -- so no need processing . Not relevant in case of Create/Update/Delete
#ProcessUserRecord(db,unitaddress, appuserid, complexid,readfortenantflag,readqrcodeflag,entitytype,entityid,regcheck,virtualroomname, idcardnum,ntransaction):
def ProcessUserRecord(db,residentdetailsid, appuserid, readfortenantflag,readqrcodeflag,entitytype,entityid,regcheck,virtualroomname, idcardnum,ntransaction,readRegistryflag):
    #fieldnameusercollection = ['name','qrcode','photolink',"[C-{"+entityid+ "}]"]

    unitaddress = getunitaddressfromresidentdetails(residentdetailsid)
    profilefieldname = None
    
    if entitytype =="COMPLEXES":
        if regcheck:
            profilefieldname= "C_R_" + 	entityid
        else:			
            profilefieldname= "C_" + 	entityid
    else:
        if regcheck:
            profilefieldname= "S_R_" + 	entityid
        else:			
            profilefieldname= "S_" + 	entityid
	

    existingentry =None
    name =None
    qrcode =None
    photolink =None
    matchedresidentunit =  None
    residentunitswithoutunitaddress =  None
    channels_ownertennantwithoutunitaddress =  None
    channels_virtualroomwithoutidcardnum =  None
    ownergroup =None
    vehqrcode_complete =None
    ownertenantchannelid = None
    operationtype=None	
    qrcodesendnotificationwithoutunitaddress = None
    channels_virtualroom = None	
    sharedwithid = None
    qrcodetotal_entry =None
    registrydata =None
    sharedwith =None	
    try:
        user_ref =db.collection("USERS").document(appuserid)
        doc12 = user_ref.get(transaction=ntransaction)
        #Add error check throw exception
        mdoc =doc12.to_dict()
        #mdoc1 =None
        #for x in doc12:
        #    mdoc1 =x
        #mdoc =mdoc1.to_dict()
        print(mdoc)
        if mdoc == None:
            print("appuserid is not defined")
        name =mdoc["name"]
        qrcode =mdoc["qrcode"]
        if 'photolink' in mdoc:
            photolink = mdoc["photolink"]
        if profilefieldname in mdoc:
            existingentry = mdoc[profilefieldname]

            if regcheck and "residentunits" in 	existingentry  and existingentry["residentunits"] is not None:
                residentunitswithoutunitaddress =  []
                for item in existingentry["residentunits"] :
                    if item["rd"] != 	residentdetailsid:
                        residentunitswithoutunitaddress.append(item)
                    else:
                         matchedresidentunit = item
                         					
                if len(residentunitswithoutunitaddress) ==0:
                    residentunitswithoutunitaddress=None               				
            

            if regcheck and "channels_oc" in 	existingentry and existingentry["channels_oc"] is not None :
                channels_ownertennantwithoutunitaddress =  []
                for item in existingentry["channels_oc"] :
                    print (item)
                    if item["unitaddress"] != 	unitaddress:
                        channels_ownertennantwithoutunitaddress.append(item)					
                if len(channels_ownertennantwithoutunitaddress) ==0:
                    channels_ownertennantwithoutunitaddress=None               				

            if regcheck and "channels_vr" in 	existingentry and existingentry["channels_vr"] is not None :
                channels_virtualroomwithoutunitaddress =  []
                for item in existingentry["channels_vr"] :
                    print (item)
                    if item["idcardnum"] != idcardnum:
                        channels_virtualroomwithoutunitaddress.append(item)					
                if len(channels_virtualroomwithoutunitaddress) ==0:
                    channels_virtualroomwithoutunitaddress=None               				

        	
    except Exception as e:
            print (e )
            print(traceback.format_exc())

    if existingentry is not None and residentdetailsid is not None:
        primaryuserid,sharedwith  = getsharedwithFromResidentUnitGroup(db,residentdetailsid,entitytype,entityid,ntransaction)

    ownertenantchannelid=None
    ownertenantchannel=None
    owneruserid=None
    residentuserid=None
    if existingentry is None:
        operationtype="insert"
    else:
        operationtype="update"

    if regcheck and readRegistryflag:
        fieldnameforregistry =["owneruserid","residentuserid"]
        owneruserid=None
        residentuserid=None
        try:


            #doc12 =  db.collection(entitytype).document(entityid).collection("REGISTRY").document(unitaddress).get(fieldnameforregistry,transaction=ntransaction,)
            doc1_ref = db.collection(entitytype).document(entityid).collection("REGISTRY").document(unitaddress)
            doc12 = doc1_ref.get(transaction=ntransaction)
            if doc12.exists:
                mdoc =doc12.to_dict()
                registrydata =mdoc
                if 'owneruserid' in registrydata:
                    owneruserid =registrydata["owneruserid"]
                if 'residentuserid' in registrydata:
                    residentuserid = registrydata["residentuserid"]
        
        except Exception as e:
            print (e )
            print(traceback.format_exc())
#check for owneruserid is for the case for SingleUserId - where we dont want to create channels between owner and user
    if regcheck and readfortenantflag and owneruserid is not None:
        try:
            doc1_ref = db.collection(entitytype).document(entityid).collection("UNITS").document(unitaddress)

            doc12 = doc1_ref.get(transaction=ntransaction)
            if doc12.exists:
                mdoc =doc12.to_dict()
                if 'channel' in mdoc:
                    ownertenantchannelid =mdoc["channel"]
                if ownertenantchannelid is not None:
                    ownertenantchannel = {"channel":ownertenantchannelid,"rights":"rw","unitaddress":unitaddress}
        except Exception as e:
            print (e )
            print(traceback.format_exc())
#relatedentry=[{"type":"m","startdate":staffdata["startdate"],"enddate":staffdata["enddate"],"isc":False}]
    qrcodeexist =False
    if readqrcodeflag and qrcode is not None:
        try:
            doc1_ref = db.collection(entitytype).document(entityid).collection("QRCODE").document(qrcode)
            doc12 = doc1_ref.get(transaction=ntransaction)
            if doc12.exists == True:
                qrcodetotal_entry =doc12.to_dict()
        except Exception as e:
            print (e )
            print(traceback.format_exc())
    return operationtype,name,qrcode,photolink, existingentry,matchedresidentunit,residentunitswithoutunitaddress,channels_ownertennantwithoutunitaddress,ownertenantchannel,owneruserid,residentuserid,qrcodesendnotificationwithoutunitaddress,vehqrcode_complete,ownergroup,qrcodetotal_entry,registrydata,sharedwith


def readQRCodeDataHelper(db,entitytype,entityid,mqrcode,transaction):
    doc1 = transaction.get(db.collection(entitytype).document(entityid).collection("QRCODE").document(mqrcode))
    doc12 =None
    for x in doc1:
        doc12 =x
    if doc12.exists == True:
        qrcodetotal_entry =doc12.to_dict()
        return qrcodetotal_entry
    return None


def AddNewStaff(batch,db,staffdata,staffid,entitytype,entityid):
    
    profileid = None
    if entitytype == "COMPLEXES":
        profileid = "C_" + entityid
    else:
        profileid = "S_" + entityid
    newuser,name,qrcode,photolink, existingentry,matchedresidentunit,residentunitswithoutunitaddress,channels_ownertennantwithoutunitaddress,ownertenantchannel,owneruserid,residentuserid,qrcodesendnotificationwithoutunitaddress,vehqrcode_complete,ownergroup,qrcodetotal_entry,registrydata,sharedwith =ProcessUserRecord(db,None, staffdata["appuserid"], False,True,entitytype,entityid,False,None, None,batch,False)
    mroles = staffdata["allowedroles"]
    channels_entity,channels_communicate,channels_supplyer,updatelist,chdata=RoleBasedChannelListFromEntity(db,mroles,entitytype,entityid,batch,None)

    print(channels_entity)
    
    if newuser:
        addEntityToUserProfile(db,newuser,staffid, channels_entity ,None, None,True,entitytype,entityid,None,mroles,channels_supplyer,channels_communicate,True,profileid,[],None, None,batch,None)
    else:
        print("error in staff addition for " +staffid )	
        
	#for qrcode - isc means the current startdate and enddate corresponds to this entry
    handleqrcodeEntry(qrcode,name,photolink,staffid,staffdata["startdate"],staffdata["enddate"],entitytype,entityid,staffqrcodetype,None,None,qrcodetotal_entry,db,batch,"insert",None)
    return id,None



def UpdateStaff(transaction,db,staffid,entitytype,entityid,olddata,newdata,byuserid):
    id = staffid
    errorstring = None
    profileid = None
    if entitytype == "COMPLEXES":
        profileid = "C_" + entityid
    else:
        profileid = "S_" + entityid

    if "allowedroles" in newdata or "enddate" in newdata:
        #newuser,name,qrcode,photolink, existingentry,matchedresidentunit,residentunitswithoutunitaddress,channels_ownertennantwithoutunitaddress,ownertenantchannel,owneruserid,residentuserid,qrcodesendnotificationwithoutunitaddress,vehqrcode_complete,ownergroup,qrcodetotal_entry,registrydata,sharedwith =ProcessUserRecord(db,None, staffid, False,True,"COMPLEXES",entityid,False,None, None,transaction,False)
        channels_entity =None
        channels_communicate = None
        channels_supplyer= None
        updatelist =None
        chdata = None
        mroles = None
        qrcodetotal_entry =None
        
        if "allowedroles" in newdata:
            mroles = newdata["allowedroles"]
            channels_entity,channels_communicate,channels_supplyer,updatelist,chdata=RoleBasedChannelListFromEntity(db,mroles,entitytype,entityid,transaction,None)
        if "enddate" in newdata:
            qrcodetotal_entry = readQRCodeDataHelper(db,entitytype,entityid,staffid,transaction)

        if "enddate" in newdata:
            enddatevalue = newdata["enddate"]
            vehcount,sercount = checkVehicleAndServiceRequestEndDate(db,entitytype,entityid,enddatevalue,staffid)
            if (vehcount > 0  or sercount > 0):
                return id, "Existing vehicles {} service request {} exists, cant terminate".format(vehcount,sercount) 
            shiftq = checkShiftPlanRequestEndDate(db,entitytype,entityid,enddatevalue,staffid)
            if (shiftq):
                return id, "Part of existing {} shifplan, cant terminate".format(shiftq)
            
            handleqrcodeEntry(staffid,None,None,None,None,enddatevalue,entitytype,entityid,"m",None,None,qrcodetotal_entry,db,transaction,"update",None)
        if "allowedroles" in newdata:

            addEntityToUserProfile(db,"update",staffid, channels_entity ,None, None,None,entitytype,entityid,None,mroles,channels_supplyer,channels_communicate,None,profileid,updatelist,None, None,transaction,None)

    
    transaction.update(db.collection(entitytype).document(entityid).collection("STAFF").document(staffid),newdata)
    
    return id,None

def UpdateResidentDetails(transaction,db,registeras,residentid,unitaddress,entitytype,entityid,olddata,newdata,byuserid):
    id = residentid
    errorstring = None
    if entitytype == "COMPLEXES":
        profileid = "C_R_" + entityid
    else:
        profileid ="error"
    residentdetailsid= unitaddress
    if registeras == "owner":
        residentdetailsid=residentdetailsid+"_o"
    else:
        residentdetailsid=residentdetailsid+"_r"

    mangementpositionfieldname =registeras+"managementposition"
    if "managementposition" in newdata:
        newdata[mangementpositionfieldname] = newdata["managementposition"]
    enddatefieldname = registeras+"enddate"
    if "enddate" in newdata:
        newdata[enddatefieldname] = newdata["enddate"]

    publishedcontactfieldname = registeras + "publishedcontact"
    if "publishedcontact" in newdata:
        newdata[publishedcontactfieldname] = newdata["publishedcontact"]



    registrynewdata={}
    updateregistryflag=False
    # we have readqrflag - False , we read qrcode via seperate function , if its required
    if mangementpositionfieldname in newdata or enddatefieldname in newdata or publishedcontactfieldname in newdata  :
        mroles = None
        changeflag=False
        newuser,name,qrcode,photolink, existingentry,matchedresidentunit,residentunitswithoutunitaddress,channels_ownertennantwithoutunitaddress,ownertenantchannel,owneruserid,residentuserid,qrcodesendnotificationwithoutunitaddress,vehqrcode_complete,ownergroup,qrcodetotal_entry,registrydata,sharedwith =ProcessUserRecord(db,residentdetailsid, residentid, False,False,"COMPLEXES",entityid,True,None, None,transaction,False)
        if mangementpositionfieldname in newdata:
            if existingentry is not None and "roles" in 	existingentry  and existingentry["roles"] is not None:
                mroles = existingentry["roles"]
                 
                if matchedresidentunit is not None:
                    matchedresidentunit["m"] = newdata[mangementpositionfieldname]
                    
                
                if "management" in mroles and (newdata[mangementpositionfieldname] is None ):
                    changeflag=True
                    mroles.remove("management")
                elif   "management" not  in mroles and (newdata[mangementpositionfieldname] is not None ) :
                    changeflag=True
                    mroles.append("management")

            if changeflag:
                channels_entity,channels_communicate,channels_supplyer,updatelist,chdata=RoleBasedChannelListFromEntity(db,mroles,entitytype,entityid,transaction,None)
        if enddatefieldname in newdata:
            givendate = newdata[enddatefieldname]
            
            vehcount,sercount = checkVehicleAndServiceRequestEndDate(db,entitytype,entityid,givendate,residentdetailsid)
            if (vehcount > 0  or sercount > 0):
                return id, "Existing vehicles {} service request {} exists with end date later than enddate specified".format(vehcount,sercount) 

        
        ###Now Writing happens
        if enddatefieldname in newdata:
            givendate = newdata[enddatefieldname]
            updateregistryflag=True
            registrynewdata[enddatefieldname]=givendate
            qentry = readQRCodeDataHelper(db,entitytype,entityid,qrcode,transaction)
            handleqrcodeEntry(qrcode,None,None,None,None,givendate,entitytype,entityid,"r",residentdetailsid,None,qentry,db,transaction,"update",sharedwith)

        if mangementpositionfieldname in newdata and changeflag:
            sharedwith = None
            addEntityToUserProfile(db,"update",residentid, channels_entity ,None,None,None,entitytype,entityid,None,mroles,channels_supplyer,channels_communicate,None,profileid,updatelist,None, None,transaction,sharedwith)
        if mangementpositionfieldname in newdata:
            registrynewdata[mangementpositionfieldname]=newdata[mangementpositionfieldname]
            updateregistryflag=True
        if publishedcontactfieldname in newdata:
            registrynewdata[publishedcontactfieldname]=newdata[publishedcontactfieldname]
            updateregistryflag=True
        if updateregistryflag==True:
            transaction.update(db.collection(entitytype).document(entityid).collection("REGISTRY").document(unitaddress),registrynewdata)
    
    return id,None





def DeleteStaff(transaction,db,staffid,entitytype,entityid,byuserid):
    id=staffid
    #check if there is any active service request or any vehicle
    enddatevalue =calendar.timegm(datetime.utcnow().date().timetuple())
    vehcount,sercount = checkVehicleAndServiceRequestEndDate(db,entitytype,entityid,enddatevalue,staffid)
    if (vehcount > 0  or sercount > 0):
        return id, "Existing vehicles {} service request {} exists, cant terminate".format(vehcount,sercount) 
    shiftq = checkShiftPlanRequestEndDate(db,entitytype,entityid,enddatevalue,staffid)
    if (shiftq):
        return id, "Part of existing {} shifplan, cant terminate".format(shiftq)
    
    newuser,name,qrcode,photolink, existingentry,matchedresidentunit,residentunitswithoutunitaddress,channels_ownertennantwithoutunitaddress,ownertenantchannel,owneruserid,residentuserid,qrcodesendnotificationwithoutunitaddress,vehqrcode_complete,ownergroup,qrcodetotal_entry,nw,sw =ProcessUserRecord(db,None, staffid, False,True,"COMPLEXES",entityid,False,None, None,transaction,False)
    doc12 = db.collection(entitytype).document(entityid).collection("STAFF").document(staffid).get(transaction=transaction)
    if doc12.exists == True:
        staffdata =doc12.to_dict()
        archivedata(staffdata,entitytype,entityid,byuserid,"STAFF",staffid,transaction,db)

    qrcodetype= "m" #-- home help
    handleqrcodeEntry(staffid, None,None,None,None,None,entitytype,entityid,qrcodetype,None,None,qrcodetotal_entry,db,transaction,"delete",None)
    entityfield = "C_"+ entityid
    if entitytype == "SERVICEPROVIDERINFO":
        entityfield = "S_"+ entityid

    
    transaction.update(db.collection(entitytype).document(entityid).collection("internaldata").document("first"),{"users":ArrayRemove([staffid])})
    transaction.delete(db.collection(entitytype).document(entityid).collection("STAFF").document(staffid))
    transaction.update(db.collection(u"USERS").document(staffid),{entityfield:firestore.DELETE_FIELD})

    return id,None
        
def copyOwnerRegistryDataToResident(regdata):
    mydata={}
    mydata["residentname"] =regdata["ownername"]
    mydata["residentuserid"] =regdata["owneruserid"]
    mydata["residenttoken"] = regdata["ownertoken"]
    mydata["residentpublishedcontact"] =regdata["ownerpublishedcontact"]
    mydata["ismanagement"] =None
    mydata["residentrecvmsg"] = regdata["ownerrecvmsg"]
    mydata["serversidetimestamp"] =firestore.SERVER_TIMESTAMP              
    return mydata

def updateUserProfileWithSharedProfileConsideration(db,concerneduserid,sharedwith,data,transaction,isserviceuser,operation,entitytype,entityid,entityfield):
    if operation == "remove":

        transaction.update(db.collection(u"USERS").document(concerneduserid),{entityfield:firestore.DELETE_FIELD})
        marray =["concerneduserid"]
        if sharedwith is not None:
            marray.append(sharedwith)
            for u in sharedwith:
                transaction.update(db.collection(u"USERS").document(u),{entityfield:firestore.DELETE_FIELD})
        
        if isserviceuser==True:
            transaction.update(db.collection(entitytype).document(entityid).collection("internaldata").document("first"),{"rusers":ArrayRemove(marray)})
        else:
            transaction.update(db.collection(entitytype).document(entityid).collection("internaldata").document("first"),{"users":ArrayRemove(marray)})

    else:
        transaction.update(db.collection("USERS").document(concerneduserid),data)
        if sharedwith is not None:
            for userid in sharedwith:
                transaction.update(db.collection("USERS").document(userid),data)



def OwnerTenantChannelOperation(db,owneruserid,sharedwith,ownertenantchannel,entkey,unitaddress,operation,transaction,entitytype,entityid):
    if ownertenantchannel is None:
        return;    
    if operation=="add":
        #myfield ={entkey+"."+	"channels_oc":ArrayUnion([ownertenantchannel]),entkey+"."+	"residentunits":ArrayUnion([unitaddress])}
        myfield ={entkey+"."+	"channels_oc":ArrayUnion([ownertenantchannel])}
        updateUserProfileWithSharedProfileConsideration(db,owneruserid,sharedwith,myfield,transaction,True,"update",entitytype,entityid,entkey)
    elif operation=="remove":
        #myfield ={entkey+"."+	"channels_oc":ArrayRemove([ownertenantchannel]),entkey+"."+	"residentunits":ArrayRemove([unitaddress])}
        myfield ={entkey+"."+	"channels_oc":ArrayRemove([ownertenantchannel])}

        updateUserProfileWithSharedProfileConsideration(db,owneruserid,sharedwith,myfield,transaction,True,"update",entitytype,entityid,entkey)


def getUsersOfEntity(db,transaction,entitytype,entityid):
    internaldata_ref = db.collection(entitytype).document(entityid).collection(u"internaldata").document("first")
    doc1 = transaction.get(internaldata_ref)
    doc12 =None
    userlist=None
    ruserlist =None
    for x in doc1:
        doc12 =x
    if doc12.exists == True:
        internaldata =doc12.to_dict()
        if "users" in internaldata:
            userlist = internaldata["users"]
        
        if "rusers" in internaldata:
            ruserlist = internaldata["rusers"]
    return userlist,ruserlist     


def EntityManagementOperationsUserProspective(db,transaction,entitytype,entityid,byuserid,operation):
    userlist,ruserlist = getUsersOfEntity(db,transaction,entitytype,entityid)
    userkey = "C_" + entityid
    ruserkey = "C_R_" + entityid
    if entitytype == "SERVICEPROVIDERINFO":
        userkey = "S_" + entityid
        ruserkey = "S_R_" + entityid
    
        if userlist is not None:
            for user in userlist:
                if operation=="suspend":
                    transaction.update(db.collection("USERS").document(user),{userkey+".suspend":True})
                elif operation=="resume":
                    transaction.update(db.collection("USERS").document(user),{userkey+".suspend":False})
                elif operation=="terminate":
                    transaction.update(db.collection("USERS").document(user),{userkey+".suspend":firestore.DELETE_FIELD})

        if ruserlist is not None:
            for user in ruserlist:
                if operation=="suspend":
                    transaction.update(db.collection("USERS").document(user),{ruserkey+".suspend":True})
                elif operation=="resume":
                    transaction.update(db.collection("USERS").document(user),{ruserkey+".suspend":False})
                elif operation=="terminate":
                    transaction.update(db.collection("USERS").document(user),{ruserkey+".suspend":firestore.DELETE_FIELD})
                
                


         

def getRoleAndChannelForResidentBasedonResidentUnit(residentunits):
    newroles=None
    if residentunits is not None:
        newroles = []
        for ru in residentunits:
            if ru["m"] == True:
                newroles.append("management")
            if ru["rd"].endswith("_o") ==True :
                newroles.append("owner")
                
            elif ru["rd"].endswith("_r")  :
                newroles.append("resident")
    return newroles

def registryActionOnDelete(db,residetailsid,unitaddress,registeras,residentuserid,owneruserid,registrydata,transaction,entitytype,entityid,ownersharewith,ownertenantchannel,entityfield):
    if registeras=="owner":
        transaction.delete(db.collection(entitytype).document(entityid).collection("REGISTRY").document(unitaddress))
    else:
        OwnerTenantChannelOperation(db,owneruserid,ownersharewith,ownertenantchannel,entityfield,unitaddress,"remove",transaction,entitytype,entityid)
        regupdatedata= complex_registryentryResidentEmptyIt()
        transaction.update(db.collection(entitytype).document(entityid).collection("REGISTRY").document(unitaddress),regupdatedata)

def getuserwithPrimarySubscriptionforResidentialGroup(db,entitytype,entityid,grpname,transaction,residetailsid):
    doc12=transaction.get(db.collection(entitytype).document(entityid).collection("UNITGRPMEM").document(grpname).collection("DATA").where("isprimary",u"==",True).select(["appuserid"]))
    primaryuserid =None
    sharedwith = []
    for x in doc12:
        mdoc1 =x
        mdoc =mdoc1.to_dict()
        appuserid = None
        if "appuserid" in  mdoc and mdoc["appuserid"] is not None:
            appuserid = mdoc["appuserid"]
            return appuserid
    return None

def checkIfTenantExistForUnitAddress(db,entitytype,entityid,transaction,unitaddress):
    doc12=transaction.get(db.collection(entitytype).document(entityid).collection("RESIDENTDETAILS").document(unitaddress+"_r"))
    for x in doc12:
        if x.exists:
            return True
    return False

def getAllEntriesForResidentUnit(db,entitytype,entityid,transaction,residentdetailsid):
    doc12=transaction.get(db.collection(entitytype).document(entityid).collection("RESIDENTDETAILS").document(residentdetailsid))
    primaryuserid =None
    sharedwith = []
    for x in doc12:
        return True

    return False


def unshareUnitSubscription(db,residentdetailid,appuserid,entitytype,entityid,transaction,profileid):
    newdata={}
    globalsharedwith=[appuserid]
    for muser in globalsharedwith:
        if muser == appuserid:
            newuser,name,qrcode,photolink, existingentry,matchedresidentunit,residentunitswithoutunitaddress,channels_ownertennantwithoutunitaddress,ownertenantchannel,owneruserid,residentuserid,qrcodesendnotificationwithoutunitaddress,vehqrcode_complete,ownergroup,qrcodetotal_entry,registrydata,sharedwith =ProcessUserRecord(db,residentdetailid, appuserid, False,True,"COMPLEXES",entityid,True,None, None,transaction,False)
            newdata[muser] ={}
            newdata[muser]["matchedresidentunit"]=matchedresidentunit
            newdata[muser]["residentunitswithoutunitaddress"]=residentunitswithoutunitaddress
            newdata[muser]["channels_ownertennantwithoutunitaddress"]=channels_ownertennantwithoutunitaddress
            newdata[muser]["qrcodetotal_entry"] =qrcodetotal_entry
        
        newroles = getRoleAndChannelForResidentBasedonResidentUnit(newdata[muser]["residentunitswithoutunitaddress"])
        newdata[muser]["newroles"]=newroles
        if newroles is not None:
            if mcomplexdata is None:
                channels_entity,channels_communicate,channels_supplyer,updatelist,mcomplexdata=RoleBasedChannelListFromEntity(db,newroles,entitytype,entityid,transaction,mcomplexdata)
                newdata[muser]["channels_entity"] =channels_entity

###Now all read thing is done, now we can start Updating information
    qrcodetype="r"
    for muser in globalsharedwith:
        handleqrcodeEntry(muser, None,None,None,None,None,entitytype,entityid,qrcodetype,residentdetailid,None,newdata[muser]["qrcodetotal_entry"],db,transaction,"delete",None)
        if  (residentunitswithoutunitaddress is None or len(residentunitswithoutunitaddress) ==0):
            # deleting user entityid field ,rest values dont maater
            addEntityToUserProfile(db,"remove",muser, None ,None, newdata[muser]["residentunitswithoutunitaddress"],None,entitytype,entityid,newdata[muser]["channels_ownertennantwithoutunitaddress"],None,None,None,None,profileid,[],None, None,transaction,None)
        else:
            addEntityToUserProfile(db,"update",muser, None ,None, newdata[muser]["residentunitswithoutunitaddress"],None,entitytype,entityid,newdata[muser]["channels_ownertennantwithoutunitaddress"],None,None,None,None,profileid,[],None, None,transaction,None)


def DeleteResidentDetailsEntry(transaction,db,residentdetailid,appuserid,entitytype,entityid,byuserid,fromsubscriptionuser):
    id=residentdetailid
    registeras="resident"
    concerneduserid=appuserid
    registeras ="resident"
    if residentdetailid.endswith("_o"):
        registeras="owner"
    unitaddress =residentdetailid.replace("_o","").replace("_r","")
    #check if there is any active service request or any vehicle
    if fromsubscriptionuser == False:
        enddatevalue =calendar.timegm(datetime.utcnow().date().timetuple())
        vehcount,sercount = checkVehicleAndServiceRequestEndDate(db,entitytype,entityid,enddatevalue,residentdetailid)
        if (vehcount > 0  or sercount > 0):
            return id, "Existing vehicles {} service request {} exists, cant terminate".format(vehcount,sercount)
    unitaddress= getunitaddressfromresidentdetails(residentdetailid)
    doc12 = db.collection(entitytype).document(entityid).collection("REGISTRY").document(unitaddress).get(transaction=transaction)
    residentdata = None
    owneruserid=None
    residentuserid=None
    if doc12.exists == True:
        residentdata =doc12.to_dict()
        if 'owneruserid' in residentdata:
            owneruserid = residentdata["owneruserid"]

        if 'residentuserid' in residentdata:
            residentuserid = residentdata["residentuserid"]
    if unitaddress+"_o" == residentdetailid and owneruserid is  None:
        return id, "No owner exists for the unit, cant delete"
    if unitaddress+"_r" == residentdetailid and residentuserid is  None:
        return id, "No resident exists for the unit, cant delete"
    
    if (unitaddress+"_o" == residentdetailid and owneruserid is not None and (owneruserid != residentuserid)):
        return id, "Resident exists for the unit, cant delete"
    newuser,name,qrcode,photolink, existingentry,matchedresidentunit,residentunitswithoutunitaddress,channels_ownertennantwithoutunitaddress,ownertenantchannel,oid,rid,qrcodesendnotificationwithoutunitaddress,vehqrcode_complete,ownergroup,qrcodetotal_entry,tobeusedregistrydata,sharedwith =ProcessUserRecord(db,residentdetailid, residentuserid, True,True,"COMPLEXES",entityid,True,None, None,transaction,True)
    globalmatchedresidentunit = matchedresidentunit
    globalownertenantchannel = ownertenantchannel
    globalsharedwith = []
    if fromsubscriptionuser ==False:
        globalsharedwith = sharedwith

    if globalsharedwith == None:
        globalsharedwith = []
    newdata={}
    newdata[residentuserid] ={}
    newdata[residentuserid]["qrcode"]=qrcode
    newdata[residentuserid]["matchedresidentunit"]=matchedresidentunit
    newdata[residentuserid]["residentunitswithoutunitaddress"]=residentunitswithoutunitaddress
    newdata[residentuserid]["channels_ownertennantwithoutunitaddress"]=channels_ownertennantwithoutunitaddress
    newdata[residentuserid]["qrcodetotal_entry"] =qrcodetotal_entry
    globalsharedwith.append(residentuserid)
    mcomplexdata = None
    ownersharewith = None

    entityfield = "C_R_"+ entityid
    if entitytype == "SERVICEPROVIDERINFO":
        entityfield = "S_R_"+ entityid

    profileid = "C_R_"+ entityid
    #need to add check for group
    if fromsubscriptionuser ==False and registeras != "owner" and oid is not None:
        if  ownergroup is None:
            primaryowneruserid,ownersharewith  = getsharedwithFromResidentUnitGroup(db,unitaddress+"_o",entitytype,entityid,transaction)
        else:
            pass  #method not implemented
    qrcodetype= "r" #-- home help

    for muser in globalsharedwith:
        if muser != appuserid:
            newuser,name,qrcode,photolink, existingentry,matchedresidentunit,residentunitswithoutunitaddress,channels_ownertennantwithoutunitaddress,ownertenantchannel,owneruserid,residentuserid,qrcodesendnotificationwithoutunitaddress,vehqrcode_complete,ownergroup,qrcodetotal_entry,registrydata,sharedwith =ProcessUserRecord(db,residentdetailid, muser, False,True,"COMPLEXES",entityid,True,None, None,transaction,False)
            newdata[muser] ={}
            newdata[muser]["qrcode"]=qrcode
            newdata[muser]["matchedresidentunit"]=matchedresidentunit
            newdata[muser]["residentunitswithoutunitaddress"]=residentunitswithoutunitaddress
            newdata[muser]["channels_ownertennantwithoutunitaddress"]=channels_ownertennantwithoutunitaddress
            newdata[muser]["qrcodetotal_entry"] =qrcodetotal_entry
        
        newroles = getRoleAndChannelForResidentBasedonResidentUnit(newdata[muser]["residentunitswithoutunitaddress"])
        newdata[muser]["newroles"]=newroles
        if newroles is not None:
            if mcomplexdata is None:
                channels_entity,channels_communicate,channels_supplyer,updatelist,mcomplexdata=RoleBasedChannelListFromEntity(db,newroles,entitytype,entityid,transaction,mcomplexdata)
                newdata[muser]["channels_entity"] =channels_entity

###Now all read thing is done, now we can start Updating information

    for muser in globalsharedwith:
        handleqrcodeEntry(newdata[muser]["qrcode"], None,None,None,None,None,entitytype,entityid,qrcodetype,residentdetailid,None,newdata[muser]["qrcodetotal_entry"],db,transaction,"delete",None)
        if  (residentunitswithoutunitaddress is None or len(residentunitswithoutunitaddress) ==0):
            # deleting user entityid field ,rest values dont maater
            addEntityToUserProfile(db,"remove",muser, None ,None, newdata[muser]["residentunitswithoutunitaddress"],None,entitytype,entityid,newdata[muser]["channels_ownertennantwithoutunitaddress"],None,None,None,None,profileid,[],None, None,transaction,None)
        else:
            addEntityToUserProfile(db,"update",muser, None ,None, newdata[muser]["residentunitswithoutunitaddress"],None,entitytype,entityid,newdata[muser]["channels_ownertennantwithoutunitaddress"],None,None,None,None,profileid,[],None, None,transaction,None)
        

    #if delete operation for tenant ,we have to remove the owner teanant channel

    ##Do required actions for Registry + OwnerSide actions  and archive residentdetails
    if fromsubscriptionuser ==False:
        archivedata(residentdata,entitytype,entityid,byuserid,"REGISTRY",unitaddress,transaction,db)
        registryActionOnDelete(db,residentdetailid,unitaddress,registeras,residentuserid,oid,tobeusedregistrydata,transaction,entitytype,entityid,ownersharewith,globalownertenantchannel,profileid)

    return id,None






def NewServiceRequest(transaction,db,servicerequestdata,entitytype,entityid,fcmtoken):

    
    
    serq =db.collection(entitytype).document(entityid).collection('SERVICEREQUESTS').document()
    servicerequestid = serq.id
    if  servicerequestdata["startdate"] is None or servicerequestdata["enddate"]  is None:
        return None, "Startdate or enddate cannot be Null"

    if servicerequestdata["startdate"] > servicerequestdata["enddate"]:
        return None, "Startdate cannot be greater than enddate"
    residetailsid = servicerequestdata["unitId"]
    if ( residetailsid is not None):
        checkresult = checkResidentDetailsEndDate(residetailsid,servicerequestdata["enddate"],False,entitytype,entityid,db)
    else:
        checkresult = checkStaffMemberEndDate(servicerequestdata["requesterid"],servicerequestdata["enddate"],entitytype,entityid,db)
        
    if checkresult==False:
        return None, "Provided End Date is greater than tenure of entity"

    
    servicereqtype = servicerequestdata["requesttype"]
    if servicereqtype == "VISITOR" or servicereqtype == "HOMEHELP":
        #storageurl = generateqrcodeandsave(storage,"/users/"+user.uid+"/qrcode.png",user)
        if servicereqtype == "VISITOR" :
            servicerequestdata["generatedqrcode"] = servicerequestid
            servicerequestdata["qrcodeimglink"] = generateqrcodeandsave(storage,"/entity/"+entityid+"/servicereq/"+ servicerequestdata["generatedqrcode"]+".png",servicerequestdata["generatedqrcode"])
        elif servicereqtype == "HOMEHELP" :
            servicerequestdata["generatedqrcode"] = servicerequestdata["phone"]+"@"+servicerequestdata["correspondingname"]
            servicerequestdata["qrcodeimglink"] = generateqrcodeandsave(storage,"/entity/"+entityid+"/servicereq/"+ servicerequestdata["generatedqrcode"]+".png",servicerequestdata["generatedqrcode"])

        
        exitingqrcodeentry=None
        #doc_ref = db.collection(entitytype).document(entityid).collection("SERVICEREQUESTS").document(servicerequestdata["generatedqrcode"])
        doc1 = transaction.get(db.collection(entitytype).document(entityid).collection("QRCODE").document(servicerequestdata["generatedqrcode"]))
        doc =None
        for x in doc1:
            doc =x
        if doc.exists:
            exitingqrcodeentry = doc.to_dict()
        qrcodetype= "h" #-- home help
        if(servicerequestdata["requesttype"] == "VISITOR"):
            qrcodetype= "g" 

        handleqrcodeEntry(servicerequestdata["generatedqrcode"], servicerequestdata["correspondingname"],servicerequestdata["adhocvisitorphoto"],servicerequestdata["requesterid"],servicerequestdata["startdate"],servicerequestdata["enddate"],entitytype,entityid,qrcodetype,None,servicerequestid,exitingqrcodeentry,db,transaction,"insert",None)		
    elif  servicereqtype == "ADHOCVISITOR" :
        if residetailsid is None or servicerequestdata["phone"] is None or servicerequestdata["correspondingname"] is None:
            return None, "Cannot have empty residentdetailsid/Name/PhoneNumber" 
        notes=""
        if servicerequestdata["notesinstructions"] is not  None:
            notes=servicerequestdata["notesinstructions"]

        mydata={}
        mydata["reqtype"] = "ADHOCVISITOR"
        mydata["reqresponse"] ="YES"
        mydata["corname"] = servicerequestdata["correspondingname"]
        mydata["phonenum"] = servicerequestdata["phone"]
        mydata["notes"] = notes
        mydata["reqtoken"] = fcmtoken
        mydata["servicerequestid"] =serq.id
        registrationtokenlist ,registrationtokenwithid = getFCMtoken(db,entitytype,entityid,False,False,False,False, residetailsid)
        response = sendFCMMessageTo(registrationtokenlist, mydata)

    transaction.set(serq,servicerequestdata)
    return serq.id,None

def AddNewVehicle(transaction,db,vehdata,entitytype,entityid,entityreg):

    if  vehdata["startdate"] is None or vehdata["enddate"]  is None:
        return None, "Startdate or enddate cannot be Null"

    if vehdata["startdate"] > vehdata["enddate"]:
        return None, "Startdate cannot be greater than enddate"
    residetailsid = vehdata["unitaddress"]

    if ( residetailsid is not None):
        checkresult = checkResidentDetailsEndDate(residetailsid,vehdata["enddate"],False,entitytype,entityid,db)
    else:
        appuserid = vehdata["appuserid"]
        checkresult = checkStaffMemberEndDate(appuserid,vehdata["enddate"],entitytype,entityid,db)
        
    if checkresult==False:
        return None, "Provided End Date is greater than tenure of entity"



#    if ( (vehdata["isvisitor"] == True )):
#        return None,None
    entitychar ="C_"
    if entitytype== "SERVICEPROVIDERINFO":
        entitychar ="S_"
    if entityreg:
        entitychar=entitychar+"R_"

    vehowner=vehdata["ownername"]
    if vehdata["username"] is not None:
        vehowner=vehdata["username"]
    #print(mydata)

    handleqrcodeEntry(vehdata["numberplate"],vehowner,vehdata["photolink"],vehdata["appuserid"],vehdata["startdate"],vehdata["enddate"],entitytype,entityid,vehicleqrcodetype,vehdata["unitaddress"],None,None,db,transaction,"insert",None)
    transaction.set(db.collection(entitytype).document(entityid).collection(u'COMPLEXVEHICLEREG').document(vehdata["numberplate"]),vehdata)
    return vehdata["numberplate"],None

    #vehqrcode= [{"numplate":vehdata["numberplate"],"qrcode":vehdata["numberplate"],"unitaddress":vehdata["unitaddress"]}]
    #transaction.update(db.collection("USERS").document(vehdata["appuserid"]),{entitychar + entityid +"." + "vehqrcode":ArrayUnion(vehqrcode)})

def unitCreateChannel(transaction,db,unitaddress,entityid,unitdata):
    print(' in unitCreateChannel start')
    doc1 = transaction.get(db.collection(u"COMPLEXES").document(entityid))
    doc =None
    for x in doc1:
        doc =x
    
    mdoc=doc.to_dict()
    entityname = mdoc["complexName"]
    
    channel1 = createchannel(db,entityname+"_ownertenant",entityid,"complex","internalgroupchat",["owner","resident"],["owner","resident"],transaction)
    unitdata["channel"] =channel1
    transaction.set(db.collection(u"COMPLEXES").document(entityid).collection(u'UNITS').document(unitaddress),unitdata)
    #transaction.update(db.collection(u"COMPLEXES").document(entityid).collection("UNITS").document(unitaddress),{"channel": channel1})      
    print(' in unitCreateChannel end')

    return  channel1, None  


def AddNewAppUser(db,storage,username, email, phonenumber,simulated):
    user,userexistflag = checkandcreateuser(username, email, phonenumber,"123456")
    if userexistflag ==False or simulated == True:
        storageurl = generateqrcodeandsave(storage,"/users/"+user.uid+"/qrcode.png",user)
        mydata={}
        mydata["version"] =1
        mydata["name"] =user.display_name 
        mydata["qrcode"] =user.uid
        mydata["qrimglink"] = storageurl
        mydata["emailverified"] =False
        mydata["phoneverified"] = False
        db.collection(u'USERS').document(user.uid).set(mydata)
    return user.uid

def CreateVirtualRoomNewFormat(transaction,db,virtualroomdata,entitytype,entityid):
    
    sectionname=virtualroomdata["sectionname"]
    grade =virtualroomdata["grade"]
    virtualroomname=grade +"-"+sectionname
    virtualroomdata["virtualroomname"]=virtualroomname
    primaryowner = virtualroomdata["primaryowner"]
    secondardaryowner= virtualroomdata["secondaryowner"]
    virtualroomdata["runningnumber"]=1
    
    attendencetype = virtualroomdata["attendencetype"]
    profileid ="S_"+entityid
    channelid = createchannel(db,virtualroomname,entityid,"SERVICEPROVIDERINFO","vr",["staff","manager"],["staff","manager"],transaction)
    
    
    if primaryowner is not None:
        VirtualRoomChannelOperationNewFormat(db,"add",primaryowner,channelid,profileid,None,True,transaction,entitytype,entityid,virtualroomname)
        InstructorAttendenceOperation(db,"add",primaryowner,"vr",virtualroomname,attendencetype,True,transaction,entitytype,entityid)

    if secondardaryowner is not None:
        for ow in secondardaryowner:
            VirtualRoomChannelOperationNewFormat(db,"add",ow,channelid,profileid,None,False,transaction,entitytype,entityid,virtualroomname)
            InstructorAttendenceOperation(db,"add",ow,"vr",virtualroomname,attendencetype,False,transaction,entitytype,entityid)

    virtualroomdata["channelid"]=channelid
    transaction.set(db.collection(entitytype).document(entityid).collection("VIRTUALROOMS").document(virtualroomname),virtualroomdata)
    return virtualroomname,None

def UpdateVirtualRoomNewFormat(transaction,db,virtualroomname,oldvirtualroomdata,newvirtualroomdata,entitytype,entityid):
    channelid=getChannelFromVirtualRoomNewFormat(transaction,db, virtualroomname,entitytype,entityid)
    oldprimaryowner=None
    newprimaryowner=None
    oldsecondaryowner =None
    newsecondaryowner =None
    attendencetype=oldvirtualroomdata["attendencetype"]
    if 'primaryowner' in oldvirtualroomdata:
        oldprimaryowner = oldvirtualroomdata["primaryowner"]
    if 'primaryowner' in newvirtualroomdata:
        newprimaryowner = newvirtualroomdata["primaryowner"]

    if 'secondaryowner' in oldvirtualroomdata:
        oldsecondaryowner = oldvirtualroomdata["secondaryowner"]
    if 'secondaryowner' in newvirtualroomdata:
        newsecondaryowner = newvirtualroomdata["secondaryowner"]


    profileid ="S_"+entityid

    
    if'primaryowner' in newvirtualroomdata  and oldprimaryowner != newprimaryowner:
        VirtualRoomChannelOperationNewFormat(db,"remove",oldprimaryowner,channelid,profileid,True,True,transaction,entitytype,entityid,virtualroomname)         
        InstructorAttendenceOperation(db,"remove",oldprimaryowner,"vr",virtualroomname,attendencetype,True,transaction,entitytype,entityid)
        VirtualRoomChannelOperationNewFormat(db,"add",newprimaryowner,channelid,profileid,True,True,transaction,entitytype,entityid,virtualroomname)         
        InstructorAttendenceOperation(db,"add",newprimaryowner,"vr",virtualroomname,attendencetype,True,transaction,entitytype,entityid)
    
    addlistsecondary=[]
    removelistsecondary=[]
    if oldsecondaryowner is  None and newsecondaryowner is not None:
        addlistsecondary=newsecondaryowner
    elif oldsecondaryowner is not  None and newsecondaryowner is  None:
        removelistsecondary = oldsecondaryowner
    elif oldsecondaryowner is not  None and newsecondaryowner is not  None:
        for userid in oldsecondaryowner:
            if userid not in newsecondaryowner:
                removelistsecondary.append(userid)

        for userid in newsecondaryowner:
            if userid not in oldsecondaryowner:
                addlistsecondary.append(userid)

    for userid in removelistsecondary:
        VirtualRoomChannelOperationNewFormat(db,"remove",userid,channelid,profileid,False,False,transaction,entitytype,entityid,virtualroomname)         
        InstructorAttendenceOperation(db,"remove",userid,"vr",virtualroomname,attendencetype,False,transaction,entitytype,entityid)


    for userid in addlistsecondary:
        VirtualRoomChannelOperationNewFormat(db,"add",userid,channelid,profileid,None,False,transaction,entitytype,entityid,virtualroomname)         
        InstructorAttendenceOperation(db,"add",userid,"vr",virtualroomname,attendencetype,False,transaction,entitytype,entityid)



    transaction.update(db.collection(entitytype).document(entityid).collection("VIRTUALROOMS").document(virtualroomname),newvirtualroomdata)
    return virtualroomname,None

def DeleteVirtualRoomNewFormat(transaction,db,virtualroomdata,entitytype,entityid):
    virtualroomname=virtualroomdata["virtualroomname"]
    channelid=getChannelFromVirtualRoomNewFormat(transaction,db, virtualroomname,entitytype,entityid)
    primaryowner = virtualroomdata["primaryowner"]
    secondardaryowner= virtualroomdata["secondaryowner"]
    attendencetype=virtualroomdata["attendencetype"]
    profileid ="S_"+entityid
    channelid=virtualroomdata["channelid"]
    if primaryowner is not None:
        VirtualRoomChannelOperationNewFormat(db,"remove",primaryowner,channelid,profileid,True,True,transaction,entitytype,entityid,virtualroomname)
        InstructorAttendenceOperation(db,"remove",primaryowner,"vr",virtualroomname,attendencetype,True,transaction,entitytype,entityid)

    if secondardaryowner is not None:
        for ow in secondardaryowner:
            VirtualRoomChannelOperationNewFormat(db,"remove",ow,channelid,profileid,False,False,transaction,entitytype,entityid,virtualroomname)
            InstructorAttendenceOperation(db,"remove",ow,"vr",virtualroomname,attendencetype,False,transaction,entitytype,entityid)
    transaction.delete(db.collection(entitytype).document(entityid).collection("VIRTUALROOMS").document(virtualroomname))
    return virtualroomname,None

def VirtualRoomChannelOperationNewFormat(db,operation,ownerinfo,virtualroomchannel,entkey,oldisprimaryval,newprimaryval,transaction,entitytype,entityid,virtualroomname):
    if virtualroomchannel is None:
        return; 
    
    vr ={}
    vr["channel"]=virtualroomchannel
    vr["isp"]=newprimaryval
    vr["vr"]=virtualroomname
    

    if operation=="add":
        mydata ={entkey+"."+	"channels_vr":ArrayUnion([vr])}

    elif operation=="remove":
        mydata ={entkey+"."+	"channels_vr":ArrayRemove([vr])}
    elif operation == 'update':
        vr1 ={}
        vr1["channel"]=virtualroomchannel
        vr1["isp"]=oldisprimaryval
        vr1["vr"]=virtualroomname

        mydata ={entkey+"."+	"channels_vr":ArrayRemove([vr1]),entkey+"."+	"channels_vr":ArrayUnion([vr])}

    ##update user profile - virtual room channel
    transaction.update(db.collection("USERS").document(ownerinfo['id']),mydata)



def InstructorAttendenceOperation(db,operation,ownerinfo,vr_or_ofr,dockey,attendencetype,isp,transaction,entitytype,entityid):
    pr=[]
    updatefield="satt"
    if isp:
        updatefield="patt"
    if attendencetype=="ONCE":
        pr.append("vr"+";FIRST"+";"+dockey)
    else:
        pr.append("vr"+";FIRST"+";"+dockey)
        pr.append("vr"+";SECOND"+";"+dockey)

    if operation=="add":
        mydata ={updatefield:ArrayUnion(pr)}
    elif operation=="remove":
        mydata ={updatefield:ArrayRemove(pr)}

    ##update user profile - virtual room channel
    transaction.set(db.collection(entitytype).document(entityid).collection("STAFFEXTRADATA").document(ownerinfo['id']),mydata, merge=True)

#cannot change vrlist if offering is of type - V and has channel =true 

def CreateOfferingScheduleNewFormat(transaction,db,offeringschdata,entitytype,entityid):
    offeringname=offeringschdata["ofrgid"]
    primaryowner = offeringschdata["primaryowner"]
    secondardaryowner= offeringschdata["secondaryowner"]
    vrlist =offeringschdata["vrlist"]
    myArray = offeringname.split("@")
    arraycount = len(myArray)
    vstr =";"
    for vs in vrlist:
        vstr=vstr +vs+ "@"

    if arraycount < 7:
        return None,"Offering name does not follow standard"
    
    offeringkind =myArray[2]
    haschannle = myArray[5]
    hasattendence =myArray[4]
    virtualroomname=None
    if vrlist ==None or len(vrlist)==0:
        return None,"Only  virtual room list cannot be empty"
    if offeringkind =="V" and len(vrlist) !=1:
        return None,"Only 1 virtual room can be allocated for above offering"
    else:
        virtualroomname=vrlist[0]
    
    offschkey= offeringname
    profileid ="S_"+entityid
    channelid=None
    if offeringkind =="V" and   haschannle=="Y":

        channelid=getChannelFromVirtualRoomNewFormat(transaction,db, virtualroomname,entitytype,entityid)
        if channelid is None:
            return None," Virtual Room is not defined properly"
    else:
        if haschannle=="Y":
            channelid = createchannel(db,primaryowner["display"]+":"+offeringname,entityid,"SERVICEPROVIDERINFO","ofr",["staff","manager"],["staff","manager"],transaction)
            offeringschdata["channelid_c"]=True

    ###Channel Operation - Update happens in user profile
    if primaryowner is not None and haschannle=="Y":
        OfferingScheduleChannelOperationNewFormat(db,"add",primaryowner,channelid,profileid,True,True,transaction,entitytype,entityid,offschkey)
         

    if secondardaryowner is not None and haschannle=="Y":
        for ow in secondardaryowner:
            OfferingScheduleChannelOperationNewFormat(db,"add",ow,channelid,profileid,False,False,transaction,entitytype,entityid,offschkey)
             


    ###Instructor specific data - for offering and attendence

    if primaryowner is not None:
        InstructorOfferingOperation(db,"add",offschkey+vstr,primaryowner,True,transaction,entitytype,entityid,hasattendence)         

    if secondardaryowner is not None:
        for ow in secondardaryowner:
            InstructorOfferingOperation(db,"add",offschkey+vstr,ow,False,transaction,entitytype,entityid,hasattendence)


    offeringschdata["channelid"]=channelid
    ofrref =db.collection(entitytype).document(entityid).collection("TEACHEROFFERINGASSIGNMENT").document(offschkey)
    transaction.set(ofrref,offeringschdata)
    return virtualroomname,None


def UpdateOfferingScheduleNewFormat(transaction,db,offeringschkey,oldofferingschdata,newofferingschdata,entitytype,entityid):
    schdata=getOfferingDataNewFormat(transaction,db,offeringschkey,entitytype,entityid)
    channelid =schdata['channelid']
    vrlist =schdata["vrlist"]
    oldprimaryowner=None
    newprimaryowner=None
    oldsecondaryowner =None
    newsecondaryowner =None
    offeringname=offeringschkey

    
    vstr=";"
    for vs in vrlist:
        vstr=vstr +vs+ "@"


    #cannot change vrlist if offering is of type - V and has channel =true 
    myArray = offeringname.split("@")
    arraycount = len(myArray)

    if arraycount < 7:
        return None,"Offering name does not follow standard"
    
    offeringkind =myArray[2]
    haschannle = myArray[5]
    hasattendence =myArray[4]
    profileid ="S_"+entityid
    if offeringkind =="V" and 'vrlist' in newofferingschdata:
        return None,"Vrlist cannot be updated once set for standard offering"

    if 'primaryowner' in newofferingschdata:
        newprimaryowner = newofferingschdata["primaryowner"]
        oldprimaryowner = oldofferingschdata["primaryowner"] 
        #channel operation       
        if channelid !=None:
            OfferingScheduleChannelOperationNewFormat(db,"remove",oldprimaryowner,channelid,profileid,True,True,transaction,entitytype,entityid,offeringschkey)
            OfferingScheduleChannelOperationNewFormat(db,"add",newprimaryowner,channelid,profileid,None,True,transaction,entitytype,entityid,offeringschkey)
        # offering operation
        InstructorOfferingOperation(db,"remove",offeringschkey+vstr,oldprimaryowner,True,transaction,entitytype,entityid,hasattendence)         
        InstructorOfferingOperation(db,"add",offeringschkey+vstr,newprimaryowner,True,transaction,entitytype,entityid,hasattendence)         

    if 'secondaryowner' in oldofferingschdata:
        oldsecondaryowner = oldofferingschdata["secondaryowner"]
    if 'secondaryowner' in newofferingschdata:
        newsecondaryowner = newofferingschdata["secondaryowner"]

    
    addlistsecondary=[]
    removelistsecondary=[]
    if oldsecondaryowner is  None and newsecondaryowner is not None:
        addlistsecondary=newsecondaryowner
    elif oldsecondaryowner is not  None and newsecondaryowner is  None:
        removelistsecondary=oldsecondaryowner
    elif oldsecondaryowner is not  None and newsecondaryowner is not  None:
        for userid in oldsecondaryowner:
            if userid not in newsecondaryowner:
                removelistsecondary.append(userid)

        for userid in newsecondaryowner:
            if userid not in oldsecondaryowner:
                addlistsecondary.append(userid)


    if channelid !=None:

        for userid in removelistsecondary:
            OfferingScheduleChannelOperationNewFormat(db,"remove",userid,channelid,profileid,False,False,transaction,entitytype,entityid,offeringschkey)

        for userid in addlistsecondary:
            OfferingScheduleChannelOperationNewFormat(db,"add",userid,channelid,profileid,False,False,transaction,entitytype,entityid,offeringschkey)
    
    for userid in removelistsecondary:
        InstructorOfferingOperation(db,"remove",offeringschkey+vstr,userid,False,transaction,entitytype,entityid,hasattendence)
    #Offering operation
    for userid in addlistsecondary:
        InstructorOfferingOperation(db,"add",offeringschkey+vstr,userid,False,transaction,entitytype,entityid,hasattendence)         


    recordref = db.collection(entitytype).document(entityid).collection("TEACHEROFFERINGASSIGNMENT").document(offeringschkey)
    transaction.update(recordref,newofferingschdata)
    return offeringschkey,None

def DeleteOfferingScheduleNewFormat(transaction,db,offeringroomdata,entitytype,entityid):
    offeringname=offeringroomdata["ofrgid"]
    primaryowner = offeringroomdata["primaryowner"]
    secondardaryowner= offeringroomdata["secondaryowner"]
    vrlist =offeringroomdata["vrlist"]
    vstr=";"
    for vs in vrlist:
        vstr=vstr +vs+ "@"

    myArray = offeringname.split("@")
    arraycount = len(myArray)

    if arraycount < 7:
        return None,"Offering name does not follow standard"
    
    offeringkind =myArray[2]
    haschannle = myArray[5]
    hasattendence =myArray[4]


    offschkey= offeringname
    profileid ="S_"+entityid

    channelid=offeringroomdata["channelid"]
    #channel operation
    #(db,operation,ownerinfo,offeringchannel,entkey,oldisprimaryval,newprimaryval,transaction,entitytype,entityid,ofrschname)    
    if channelid !=None:
        if primaryowner is not None:
            OfferingScheduleChannelOperationNewFormat(db,"remove",primaryowner,channelid,profileid,True,True,transaction,entitytype,entityid,offschkey)         

        if secondardaryowner is not None:
            for ow in secondardaryowner:
                OfferingScheduleChannelOperationNewFormat(db,"remove",ow,channelid,profileid,False,False,transaction,entitytype,entityid,offschkey)             

    #offering opertation
    #InstructorOfferingOperation(db,operation,offschkey,ownerinfo,isp,transaction,entitytype,entityid)
    if primaryowner is not None:
        InstructorOfferingOperation(db,"remove",offschkey+vstr,primaryowner,True,transaction,entitytype,entityid,hasattendence)         

    if secondardaryowner is not None:
        for ow in secondardaryowner:
            InstructorOfferingOperation(db,"remove",offschkey+vstr,ow,False,transaction,entitytype,entityid,hasattendence)

    transaction.delete(db.collection(entitytype).document(entityid).collection("TEACHEROFFERINGASSIGNMENT").document(offschkey))
    return offschkey,None





def OfferingScheduleChannelOperationNewFormat(db,operation,ownerinfo,offeringchannel,entkey,oldisprimaryval,newprimaryval,transaction,entitytype,entityid,ofrschname):
    if offeringchannel is None:
        return 

    vr ={}
    vr["channel"]=offeringchannel
    vr["isp"]=newprimaryval
    vr["ofr"]=ofrschname


    if operation=="add":
        mydata ={entkey+"."+	"channels_oc":ArrayUnion([vr])}

    elif operation=="remove":
        mydata ={entkey+"."+	"channels_oc":ArrayRemove([vr])}


    ##update user profile - virtual room channel
    print("operation is " + operation + " data is " + ofrschname + " isp set to " +str(newprimaryval)  + " offeringchannel is " + offeringchannel + " id is " + ownerinfo['id'])
    docref =db.collection("USERS").document(ownerinfo['id'])
    transaction.update(docref,mydata)

def InstructorOfferingOperation(db,operation,offschkey,ownerinfo,isp,transaction,entitytype,entityid,hasattendence):

    updatefield="sofr"
    updateatt="satt"
    if isp:
        updatefield="pofr"
        updateatt="patt"

    if operation=="add":
        if hasattendence == "Y":
            mydata ={updatefield:ArrayUnion([offschkey]),updateatt:ArrayUnion(["of"+";"+offschkey])}
        else:
            mydata ={updatefield:ArrayUnion([offschkey])}
    elif operation=="remove":
        if hasattendence == "Y":
            mydata ={updatefield:ArrayRemove([offschkey]),updateatt:ArrayRemove(["of"+";"+offschkey])}
        else:
            mydata ={updatefield:ArrayRemove([offschkey])}
    print("operation is " + operation + " data is " + offschkey + " field is " + updatefield + " id is " + ownerinfo['id'] + " attendence " + hasattendence)
    ##update user profile - virtual room channel
    transaction.set(db.collection(entitytype).document(entityid).collection("STAFFEXTRADATA").document(ownerinfo['id']),mydata, merge=True)

def getOfferingDataNewFormat(transaction,db,offerschkey,entitytype,entityid):
    docpath =entitytype+'/'+entityid+'/TEACHEROFFERINGASSIGNMENT/'+offerschkey
    print(docpath)
    doc1 = db.collection('SERVICEPROVIDERINFO').document(entityid).collection("TEACHEROFFERINGASSIGNMENT").document(offerschkey).get(transaction=transaction)
    docdata=None
    if doc1.exists:
        docdata = doc1.to_dict()
    return docdata


def getChannelFromOfferingNewFormat(transaction,db,offerschkey,entitytype,entityid):
    docpath =entitytype+'/'+entityid+'/TEACHEROFFERINGASSIGNMENT/'+offerschkey
    print(docpath)
    doc1 = transaction.get(db.collection('SERVICEPROVIDERINFO').document(entityid).collection("TEACHEROFFERINGASSIGNMENT").where(firestore1.field_path.FieldPath.document_id(), "==", db.document(docpath)).select(['channelid']))
    setflag=False
    channelid=None
    for x in doc1:
        if x.exists:
            m = x.to_dict()
            setflag=setflag+1
            if "channelid" in m:
                channelid = m["channelid"]
    return channelid


def getChannelFromVirtualRoomNewFormat(transaction,db, virtualroomname,entitytype,entityid):
    docpath =entitytype+'/'+entityid+'/VIRTUALROOMS/'+virtualroomname
    print(docpath)
    doc1 = transaction.get(db.collection('SERVICEPROVIDERINFO').document(entityid).collection("VIRTUALROOMS").where(firestore1.field_path.FieldPath.document_id(), "==", db.document(docpath)).select(['channelid']))
    setflag=False
    channelid=None
    for x in doc1:
        if x.exists:
            m = x.to_dict()
            setflag=setflag+1
            if "channelid" in m:
                channelid = m["channelid"]
    return channelid



def OfferingModelGroupRequest(transaction,db,docdata,key,actiontype,entitytype,entityid):
    data=docdata
    docref = db.collection(entitytype).document(entityid).collection("OFFERINGMODEL").document(key)
    if actiontype=='add':
        docref.set(data)
    elif actiontype =="update":
        docref.update(data)
    elif actiontype =="remove":
        #do not delete if document exist in TEACHEROFFERINGASSIGNMENT
        docref1 = db.collection(entitytype).document(entityid).collection("TEACHEROFFERINGASSIGNMENT").document(key)
        docSnapshot = docref1.get([]);
        if docSnapshot.exists:
            docref.delete()
        else:
            return None,"Cannot delete corresponding TEACHEROFFERINGASSIGNMENT exist"
    return key,None


def OfferingWeeklyScheduleRequest(transaction,db,docdata,key,actiontype,entitytype,entityid):
    data=docdata
    docref = db.collection(entitytype).document(entityid).collection("OFFERINGWEEKLYSCHEDULE").document(key)
    if actiontype=='add':
        docref.set(data)
    elif actiontype =="update":
        docref.update(data)
    elif actiontype =="remove":
        docref.delete()
    return key,None


