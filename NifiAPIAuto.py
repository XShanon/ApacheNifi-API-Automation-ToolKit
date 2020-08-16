import requests 
import json
import re


def RestRequest_2(METHOD,ENDPOINT):
   try:
      f.write("\nINFO  sendmodalert - RestRequest() -- Started")
      ### Domain ###
      f.write("\nINFO  sendmodalert - RestRequest() -- Declaring Domain")
      Domain = 

      ### Query ###
      f.write("\nINFO  sendmodalert - RestRequest() -- Declaring Query")
      Query = "/nifi-api"+ENDPOINT

      ### URL ####
      f.write("\nINFO  sendmodalert - RestRequest() -- Declaring URL")
      URL=Domain+Query

      f.write("DEBUG  sendmodalert - RestRequest() -- URL: "+str(URL))
      f.write("\nINFO  sendmodalert - RestRequest() -- Sending GET Request")
      response = requests.get(URL)
      f.write("\nINFO  sendmodalert - RestRequest() -- Recieving Ack")
      
      status = response.status_code
      f.write("\nINFO  sendmodalert - RestRequest() -- status: "+str(status))
      
      url = response.url
      f.write("\nINFO  sendmodalert - RestRequest() -- URL:"+str(url))

      #content = response.content
      #f.write("\nINFO  sendmodalert - RestRequest() -- content: "+str(content))

      #headers = response.headers
      #f.write("\nINFO  sendmodalert - RestRequest() -- headers: "+str(headers))

      #raw = response.raw
      #f.write("\nINFO  sendmodalert - RestRequest() -- raw: "+str(raw))

      reason = response.reason
      f.write("\nINFO  sendmodalert - RestRequest() -- reason: "+str(reason))

      #request = response.request
      #f.write("\nINFO  sendmodalert - RestRequest() -- request: "+str(request))

      text = response.text
      if re.match("\{.*\}", text):
          text_json=json.dumps(json.loads(text), indent=4, sort_keys=True)
          #JSON_DATA=json.load(dict(text_json))
          #print("\n---------------\n"+str(JSON_DATA['processGroupFlow']['flow']['processGroups']))
      else:
          text_json=text
      f.write("\nINFO  sendmodalert - RestRequest() -- text: \n"+str(text_json))
      #print(text_json)
      return text_json
   except (ValueError,IOError) as err:
      f.write(" - Main() -- Error Occured \n"+str(err))


def TreeTrace(parent,text_json):
    ProcessorGroups=[]
    PGDICT={}
    PDICT={}
    Prcessors=[]
    json_data = json.loads(text_json)    
    NumProcessGroups=len(json_data['processGroupFlow']['flow']['processGroups'])
    for i in range(NumProcessGroups):
        PGDICT={}
        PGName=json_data['processGroupFlow']['flow']['processGroups'][i]['component']['name']
        PGId=json_data['processGroupFlow']['flow']['processGroups'][i]['component']['id']
        PGcomments=json_data['processGroupFlow']['flow']['processGroups'][i]['component']['comments']
        PGactiveThreadCount=json_data['processGroupFlow']['flow']['processGroups'][i]['status']['aggregateSnapshot']['activeThreadCount']
        PGbytesIn=json_data['processGroupFlow']['flow']['processGroups'][i]['status']['aggregateSnapshot']['bytesIn']
        PGbytesOut=json_data['processGroupFlow']['flow']['processGroups'][i]['status']['aggregateSnapshot']['bytesOut']
        PGbytesQueued=json_data['processGroupFlow']['flow']['processGroups'][i]['status']['aggregateSnapshot']['bytesQueued']
        PGbytesRead=json_data['processGroupFlow']['flow']['processGroups'][i]['status']['aggregateSnapshot']['bytesRead']
        PGbytesReceived=json_data['processGroupFlow']['flow']['processGroups'][i]['status']['aggregateSnapshot']['bytesReceived']
        PGbytesSent=json_data['processGroupFlow']['flow']['processGroups'][i]['status']['aggregateSnapshot']['bytesSent']
        PGbytesTransferred=json_data['processGroupFlow']['flow']['processGroups'][i]['status']['aggregateSnapshot']['bytesTransferred']
        PGbytesWritten=json_data['processGroupFlow']['flow']['processGroups'][i]['status']['aggregateSnapshot']['bytesWritten']
        PGflowFilesIn=json_data['processGroupFlow']['flow']['processGroups'][i]['status']['aggregateSnapshot']['flowFilesIn']
        PGflowFilesOut=json_data['processGroupFlow']['flow']['processGroups'][i]['status']['aggregateSnapshot']['flowFilesOut']
        PGflowFilesQueued=json_data['processGroupFlow']['flow']['processGroups'][i]['status']['aggregateSnapshot']['flowFilesQueued']
        PGflowFilesReceived=json_data['processGroupFlow']['flow']['processGroups'][i]['status']['aggregateSnapshot']['flowFilesReceived']
        PGflowFilesSent=json_data['processGroupFlow']['flow']['processGroups'][i]['status']['aggregateSnapshot']['flowFilesSent']
        PGflowFilesTransferred=json_data['processGroupFlow']['flow']['processGroups'][i]['status']['aggregateSnapshot']['flowFilesTransferred']
        PGinput=json_data['processGroupFlow']['flow']['processGroups'][i]['status']['aggregateSnapshot']['input']
        PGoutput=json_data['processGroupFlow']['flow']['processGroups'][i]['status']['aggregateSnapshot']['output']
        PGqueued=json_data['processGroupFlow']['flow']['processGroups'][i]['status']['aggregateSnapshot']['queued']
        PGqueuedCount=json_data['processGroupFlow']['flow']['processGroups'][i]['status']['aggregateSnapshot']['queuedCount']
        PGqueuedSize=json_data['processGroupFlow']['flow']['processGroups'][i]['status']['aggregateSnapshot']['queuedSize']
        PGread=json_data['processGroupFlow']['flow']['processGroups'][i]['status']['aggregateSnapshot']['read']
        PGreceived=json_data['processGroupFlow']['flow']['processGroups'][i]['status']['aggregateSnapshot']['received']
        PGsent=json_data['processGroupFlow']['flow']['processGroups'][i]['status']['aggregateSnapshot']['sent']
        PGterminatedThreadCount=json_data['processGroupFlow']['flow']['processGroups'][i]['status']['aggregateSnapshot']['terminatedThreadCount']
        PGtransferred=json_data['processGroupFlow']['flow']['processGroups'][i]['status']['aggregateSnapshot']['transferred']
        PGwritten=json_data['processGroupFlow']['flow']['processGroups'][i]['status']['aggregateSnapshot']['written']        

        PGDICT.update({"Name":str(PGName)})
        PGDICT.update({"Id":str(PGId)})
        PGDICT.update({"comments":str(PGcomments)})
        PGDICT.update({"activeThreadCount":str(PGactiveThreadCount)})
        PGDICT.update({"bytesIn":str(PGbytesIn)})
        PGDICT.update({"bytesOut":str(PGbytesOut)})
        PGDICT.update({"bytesQueued":str(PGbytesQueued)})
        PGDICT.update({"bytesRead":str(PGbytesRead)})
        PGDICT.update({"bytesReceived":str(PGbytesReceived)})
        PGDICT.update({"bytesSent":str(PGbytesSent)})
        PGDICT.update({"bytesTransferred":str(PGbytesTransferred)})
        PGDICT.update({"bytesWritten":str(PGbytesWritten)})
        PGDICT.update({"flowFilesIn":str(PGflowFilesIn)})
        PGDICT.update({"flowFilesOut":str(PGflowFilesOut)})
        PGDICT.update({"flowFilesQueued":str(PGflowFilesQueued)})
        PGDICT.update({"flowFilesReceived":str(PGflowFilesReceived)})
        PGDICT.update({"flowFilesSent":str(PGflowFilesSent)})
        PGDICT.update({"flowFilesTransferred":str(PGflowFilesTransferred)})
        PGDICT.update({"input":str(PGinput)})
        PGDICT.update({"output":str(PGoutput)})
        PGDICT.update({"queued":str(PGqueued)})
        PGDICT.update({"queuedCount":str(PGqueuedCount)})
        PGDICT.update({"queuedSize":str(PGqueuedSize)})
        PGDICT.update({"read":str(PGread)})
        PGDICT.update({"received":str(PGreceived)})
        PGDICT.update({"sent":str(PGsent)})
        PGDICT.update({"terminatedThreadCount":str(PGterminatedThreadCount)})
        PGDICT.update({"transferred":str(PGtransferred)})
        PGDICT.update({"written":str(PGwritten)})

        PGDICT.update({"ParentName":str(parent["Name"])})
        PGDICT.update({"ParentId":str(parent["Id"])})
        ProcessorGroups.append(PGDICT)

        
    NumProcessGroups=len(json_data['processGroupFlow']['flow']['processors'])
    for i in range(NumProcessGroups): 
        PDICT={}
        PName=json_data['processGroupFlow']['flow']['processors'][i]['component']['name']
        PId=json_data['processGroupFlow']['flow']['processors'][i]['component']['id']
        Pcomments=json_data['processGroupFlow']['flow']['processors'][i]['component']['config']['comments']
        PactiveThreadCount=json_data['processGroupFlow']['flow']['processors'][i]["status"]["aggregateSnapshot"]['activeThreadCount']
        PbytesIn=json_data['processGroupFlow']['flow']['processors'][i]["status"]["aggregateSnapshot"]['bytesIn']
        PbytesOut=json_data['processGroupFlow']['flow']['processors'][i]["status"]["aggregateSnapshot"]['bytesOut']
        #PbytesQueued=json_data['processGroupFlow']['flow']['processors'][i]["status"]["aggregateSnapshot"]['bytesQueued']
        PbytesRead=json_data['processGroupFlow']['flow']['processors'][i]["status"]["aggregateSnapshot"]['bytesRead']
        #PbytesReceived=json_data['processGroupFlow']['flow']['processors'][i]["status"]["aggregateSnapshot"]['bytesReceived']
        #PbytesSent=json_data['processGroupFlow']['flow']['processors'][i]["status"]["aggregateSnapshot"]['bytesSent']
        #PbytesTransferred=json_data['processGroupFlow']['flow']['processors'][i]["status"]["aggregateSnapshot"]['bytesTransferred']
        PbytesWritten=json_data['processGroupFlow']['flow']['processors'][i]["status"]["aggregateSnapshot"]['bytesWritten']
        PflowFilesIn=json_data['processGroupFlow']['flow']['processors'][i]["status"]["aggregateSnapshot"]['flowFilesIn']
        PflowFilesOut=json_data['processGroupFlow']['flow']['processors'][i]["status"]["aggregateSnapshot"]['flowFilesOut']
        #PflowFilesQueued=json_data['processGroupFlow']['flow']['processors'][i]["status"]["aggregateSnapshot"]['flowFilesQueued']
        #PflowFilesReceived=json_data['processGroupFlow']['flow']['processors'][i]["status"]["aggregateSnapshot"]['flowFilesReceived']
        #PflowFilesSent=json_data['processGroupFlow']['flow']['processors'][i]["status"]["aggregateSnapshot"]['flowFilesSent']
        #PflowFilesTransferred=json_data['processGroupFlow']['flow']['processors'][i]["status"]["aggregateSnapshot"]['flowFilesTransferred']
        Pinput=json_data['processGroupFlow']['flow']['processors'][i]["status"]["aggregateSnapshot"]['input']
        Poutput=json_data['processGroupFlow']['flow']['processors'][i]["status"]["aggregateSnapshot"]['output']
        #Pqueued=json_data['processGroupFlow']['flow']['processors'][i]["status"]["aggregateSnapshot"]['queued']
        #PqueuedCount=json_data['processGroupFlow']['flow']['processors'][i]["status"]["aggregateSnapshot"]['queuedCount']
        #PqueuedSize=json_data['processGroupFlow']['flow']['processors'][i]["status"]["aggregateSnapshot"]['queuedSize']
        Pread=json_data['processGroupFlow']['flow']['processors'][i]["status"]["aggregateSnapshot"]['read']
        #Preceived=json_data['processGroupFlow']['flow']['processors'][i]["status"]["aggregateSnapshot"]['received']
        #Psent=json_data['processGroupFlow']['flow']['processors'][i]["status"]["aggregateSnapshot"]['sent']
        PterminatedThreadCount=json_data['processGroupFlow']['flow']['processors'][i]["status"]["aggregateSnapshot"]['terminatedThreadCount']
        #Ptransferred=json_data['processGroupFlow']['flow']['processors'][i]["status"]["aggregateSnapshot"]['transferred']
        Pwritten=json_data['processGroupFlow']['flow']['processors'][i]["status"]["aggregateSnapshot"]['written'] 
        
        PDICT.update({"Name":str(PName)})
        PDICT.update({"Id":str(PId)})
        PDICT.update({"Name":str(PName)})
        PDICT.update({"Id":str(PId)})
        PDICT.update({"comments":str(Pcomments)})
        PDICT.update({"activeThreadCount":str(PactiveThreadCount)})
        PDICT.update({"bytesIn":str(PbytesIn)})
        PDICT.update({"bytesOut":str(PbytesOut)})
        #PDICT.update({"bytesQueued":str(PbytesQueued)})
        PDICT.update({"bytesRead":str(PbytesRead)})
        #PDICT.update({"bytesReceived":str(PbytesReceived)})
        #PDICT.update({"bytesSent":str(PbytesSent)})
        #PDICT.update({"bytesTransferred":str(PbytesTransferred)})
        PDICT.update({"bytesWritten":str(PbytesWritten)})
        PDICT.update({"flowFilesIn":str(PflowFilesIn)})
        PDICT.update({"flowFilesOut":str(PflowFilesOut)})
        #PDICT.update({"flowFilesQueued":str(PflowFilesQueued)})
        #PDICT.update({"flowFilesReceived":str(PflowFilesReceived)})
        #PDICT.update({"flowFilesSent":str(PflowFilesSent)})
        #PDICT.update({"flowFilesTransferred":str(PflowFilesTransferred)})
        PDICT.update({"input":str(Pinput)})
        PDICT.update({"output":str(Poutput)})
        #PDICT.update({"queued":str(Pqueued)})
        #PDICT.update({"queuedCount":str(PqueuedCount)})
        #PDICT.update({"queuedSize":str(PqueuedSize)})
        PDICT.update({"read":str(Pread)})
        #PDICT.update({"received":str(Preceived)})
        #PDICT.update({"sent":str(Psent)})
        PDICT.update({"terminatedThreadCount":str(PterminatedThreadCount)})
        #PDICT.update({"transferred":str(Ptransferred)})
        PDICT.update({"written":str(Pwritten)})                        

        PDICT.update({"ParentName":str(parent["Name"])})
        PDICT.update({"ParentId":str(parent["Id"])})

        Prcessors.append(PDICT)
    return ProcessorGroups,Prcessors

f = open("Artifacts.json", "w")
file = open("lookup.csv", "a+")
#Request_Artifacts = [ {"EndPoint":"/access","Method":"GET"} ]
#Request_Artifacts = [ 
#                      {"EndPoint":"/access","Method":"GET"},
#                      {"EndPoint":"/resources","Method":"GET"},
#                      {"EndPoint":"/flow/cluster/summary","Method":"GET"},
#                      {"EndPoint":"/flow/config","Method":"GET"},
#                      {"EndPoint":"/flow/process-groups/root","Method":"GET"}
#                    ]
#for RA in Request_Artifacts:
#    f.write("\n"+"EndPoint is : "+str(RA["EndPoint"])+"\n"+"Method is : "+str(RA["Method"]))
#    print("\n"+"EndPoint is : "+str(RA["EndPoint"])+"\n"+"Method is : "+str(RA["Method"])+"\n---------------------------------------")
#    text_json=RestRequest_2(RA["Method"],RA["EndPoint"])
#    if RA["EndPoint"] == "/flow/process-groups/root":
#        ProcessorGroups,Prcessors=TreeTrace(text_json)
#        print("ProcessorGroups:\n"+str(ProcessorGroups)+"\nPrcessors:\n"+str(Prcessors))

def Proc_ProcGrp_Lookup(METHOD,ENDPOINT):
    try:
        ParentPGs=[ {"Name":"root","Id":"root"} ]
        file.write("ParentName,ParentId,ChildName,ChildId,Type,comments,activeThreadCount,bytesIn,bytesOut,bytesQueued,bytesRead,bytesReceived,bytesSent,bytesTransferred,bytesWritten,flowFilesIn,flowFilesOut,flowFilesQueued,flowFilesReceived,flowFilesSent,flowFilesTransferred,input,output,queued,queuedCount,queuedSize,read,received,sent,terminatedThreadCount,transferred,written")
        for PPGs in ParentPGs:
            print(str(PPGs))
            text_json=RestRequest_2("GET","/flow/process-groups/"+str(PPGs["Id"]))
            ProcessorGroups,Prcessors=TreeTrace(PPGs,text_json)
            for ProcGRP in ProcessorGroups:
                ParentPGs.append({"Name":str(ProcGRP["Name"]),"Id":str(ProcGRP["Id"])})
                file.write("\n"+str(ProcGRP["ParentName"])+","+str(ProcGRP["ParentId"])+","+str(ProcGRP["Name"])+","+str(ProcGRP["Id"])+",Processor_Group"+","+str(ProcGRP["comments"])+","+str(ProcGRP["activeThreadCount"])+","+str(ProcGRP["bytesIn"])+","+str(ProcGRP["bytesOut"])+","+str(ProcGRP["bytesQueued"])+","+str(ProcGRP["bytesRead"])+","+str(ProcGRP["bytesReceived"])+","+str(ProcGRP["bytesSent"])+","+str(ProcGRP["bytesTransferred"])+","+str(ProcGRP["bytesWritten"])+","+str(ProcGRP["flowFilesIn"])+","+str(ProcGRP["flowFilesOut"])+","+str(ProcGRP["flowFilesQueued"])+","+str(ProcGRP["flowFilesReceived"])+","+str(ProcGRP["flowFilesSent"])+","+str(ProcGRP["flowFilesTransferred"])+","+str(ProcGRP["input"])+","+str(ProcGRP["output"])+","+str(ProcGRP["queued"])+","+str(ProcGRP["queuedCount"])+","+str(ProcGRP["queuedSize"])+","+str(ProcGRP["read"])+","+str(ProcGRP["received"])+","+str(ProcGRP["sent"])+","+str(ProcGRP["terminatedThreadCount"])+","+str(ProcGRP["transferred"])+","+str(ProcGRP["written"]))
                #file.write("\n"+str(ProcGRP["ParentName"])+","+str(ProcGRP["ParentId"])+","+str(ProcGRP["Name"])+","+str(ProcGRP["Id"])+",Processor_Group")
            for Procs in Prcessors:
                file.write("\n"+str(Procs["ParentName"])+","+str(Procs["ParentId"])+","+str(Procs["Name"])+","+str(Procs["Id"])+",Processor"+","+str(Procs["comments"])+","+str(Procs["activeThreadCount"])+","+str(Procs["bytesIn"])+","+str(Procs["bytesOut"])+","+"None"+","+str(Procs["bytesRead"])+","+"None"+","+"None"+","+"None"+","+str(Procs["bytesWritten"])+","+str(Procs["flowFilesIn"])+","+str(Procs["flowFilesOut"])+","+"None"+","+"None"+","+"None"+","+"None"+","+str(Procs["input"])+","+str(Procs["output"])+","+"None"+","+"None"+","+"None"+","+str(Procs["read"])+","+"None"+","+"None"+","+str(Procs["terminatedThreadCount"])+","+"None"+","+str(Procs["written"]))
                #file.write("\n"+str(Procs["ParentName"])+","+str(Procs["ParentId"])+","+str(Procs["Name"])+","+str(Procs["Id"])+",Processor"+","+str(Procs["comments"])+","+str(Procs["activeThreadCount"])+","+str(Procs["bytesIn"])+","+str(Procs["bytesOut"])+","+str(Procs["bytesQueued"])+","+str(Procs["bytesRead"])+","+str(Procs["bytesReceived"])+","+str(Procs["bytesSent"])+","+str(Procs["bytesTransferred"])+","+str(Procs["bytesWritten"])+","+str(Procs["flowFilesIn"])+","+str(Procs["flowFilesOut"])+","+str(Procs["flowFilesQueued"])+","+str(Procs["flowFilesReceived"])+","+str(Procs["flowFilesSent"])+","+str(Procs["flowFilesTransferred"])+","+str(Procs["input"])+","+str(Procs["output"])+","+str(Procs["queued"])+","+str(Procs["queuedCount"])+","+str(Procs["queuedSize"])+","+str(Procs["read"])+","+str(Procs["received"])+","+str(Procs["sent"])+","+str(Procs["terminatedThreadCount"])+","+str(Procs["transferred"])+","+str(Procs["written"]))
                #file.write("\n"+str(Procs["ParentName"])+","+str(Procs["ParentId"])+","+str(Procs["Name"])+","+str(Procs["Id"])+",Processor")
            print("ProcessorGroups:\n"+str(ProcessorGroups)+"\nPrcessors:\n"+str(Prcessors))
        f.close()        
    except (ValueError,IOError) as err:
       f.write(" - Main() -- Error Occured \n"+str(err))

def Main():
    API=[ 
#               {"Method":"GET","EndPoint":"/flow/process-groups/"},
               {"Method":"GET","EndPoint":"/system-diagnostics"},
#               {"Method":"GET","EndPoint":"/flow/about"}
#               {"Method":"GET","EndPoint":"/resources"}
#               {"Method":"GET","EndPoint":"/flow/cluster/summary"}
#                {"Method":"GET","EndPoint":"/flow/status"}
             ]
    for i in range(len(API)):
        if API[i]["EndPoint"] == "/flow/process-groups/":
            Proc_ProcGrp_Lookup(API[i]["Method"],API[i]["EndPoint"])
        else:
            text_json=RestRequest_2(API[i]["Method"],API[i]["EndPoint"])
Main()