from sqlalchemy import create_engine, Column, MetaData, Table, Index
from sqlalchemy import Integer, String, Text, Float, Boolean, BigInteger, Numeric, SmallInteger
import requests
from requests_futures.sessions import FuturesSession
import requests_futures
from concurrent.futures import as_completed
import sys
import time

import logging

logging.basicConfig(filename='lpstore.log',level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s')


def RateLimited(maxPerSecond):
    minInterval = 1.0 / float(maxPerSecond)
    def decorate(func):
        lastTimeCalled = [0.0]
        def rateLimitedFunction(*args,**kargs):
            elapsed = time.clock() - lastTimeCalled[0]
            leftToWait = minInterval - elapsed
            if leftToWait>0:
                time.sleep(leftToWait)
            ret = func(*args,**kargs)
            lastTimeCalled[0] = time.clock()
            return ret
        return rateLimitedFunction
    return decorate


def processData(result,connection,metadata):
    
    offerTable = Table('lpOffers',metadata)
    requirementsTable = Table('lpOfferRequirements',metadata)

    try:
        resp=result.result()
        if resp.status_code==200:
            lpstore=resp.json()
            if lpstore['pageCount'] > 1:
                logging.fatal("Steve's a moron. {} has multiple pages".format(result.corporationid))
            for offer in lpstore['items']:
                offerres=connection.execute(offerTable.insert(),
                                    corporationID=result.corporationid,
                                    typeID=offer['item']['id'],
                                    quantity=offer['quantity'],
                                    lpCost=offer['lpCost'],
                                    iskCost=offer['iskCost']
                                )
                offerid=offerres.inserted_primary_key
                for reqitem in offer['requiredItems']:
                    connection.execute(requirementsTable.insert(),
                                    offerID=offerid,
                                    typeID=reqitem['item']['id'],
                                    quantity=reqitem['quantity']
                                    )
            return True;
        else:
            logging.warn("None 200 status. {} Returned: {}".format(result.corporationid,resp.status_code))
            return False
    except requests.exceptions.ConnectionError as e:
        logging.warn(e)
        return False;


@RateLimited(25)
def getData(requestsConnection,url,corporationid):
    future=requestsConnection.get(url)
    future.corporationid=corporationid
    return future



if __name__ == "__main__":
    
    engine = create_engine('mysql://eve:eve@localhost:3306/lpstore2', echo=False)
    metadata = MetaData()
    connection = engine.connect()

    offerTable = Table('lpOffers',metadata,
                            Column('offerID',Integer,primary_key=True, autoincrement=True),
                            Column('corporationID',Integer),
                            Column('typeID',Integer),
                            Column('quantity',Integer),
                            Column('lpCost',Integer),
                            Column('iskCost',Integer)
                      )
    requirementsTable = Table('lpOfferRequirements',metadata,
                                Column('offerID',Integer),
                                Column('typeID',Integer),
                                Column('quantity',Integer)
                            )
    Index("lpOffers_corporation",offerTable.c.corporationID)
    Index("lpOffers_typeid",offerTable.c.typeID)
    Index("lpOffers_corp_type",offerTable.c.corporationID,offerTable.c.typeID)


    Index("lpReqs_offerID",requirementsTable.c.offerID)
                            

    metadata.drop_all(engine,checkfirst=True)
    metadata.create_all(engine,checkfirst=True)




    reqs_num_workers = 10
    session = FuturesSession(max_workers=reqs_num_workers)
    session.headers.update({'UserAgent':'Fuzzwork LP Store updater'});
 
    futures=[]
    retryfutures=[]
    
    corporation_request = session.get("https://api-sisi.testeveonline.com/corporations/npccorps/")
    corporation_result = corporation_request.result() 

    if corporation_result.status_code != 200:
        logging.fatal("Bad Corporation Response")
        exit()

    corporations=corporation_result.json()
    
    for corporation in corporations['items']:
        if corporation['id']!=1000001:
            futures.append(getData(session,corporation['loyaltyStore']['href'],corporation['id']))
        
    for result in as_completed(futures):
        trans = connection.begin()
        status=processData(result,connection,metadata)
        if not status:
                logging.warn("adding {} to retry".format(result.corporationid))
        trans.commit()
