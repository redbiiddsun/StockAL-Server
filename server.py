import requests
import schedule
import time
import mysql.connector
from mysql.connector import cursor, errorcode

binanceUrl = 'https://api.binance.com/api/v3/ticker/24hr' 
bitkubUrl = 'https://api.bitkub.com/api/market/ticker'
bitkubsymbolUrl  = 'https://api.bitkub.com/api/market/symbols'
dataJSON = []
bitkubsymbolJSON = []
dictJSON = {}

insertQueryBinanace = ("INSERT INTO binance "
                        "(symbol, priceChange, priceChangePercent, weightedAvgPrice, " 
                        "prevClosePrice, lastPrice, lastQty, bidPrice, bidQty, askPrice, askQty, "
                        "openPrice, highPrice, lowPrice, volume, quoteVolume, openTime, closeTime, firstId, lastId, count) "
                        "VALUE (%(symbol)s, %(priceChange)s, %(priceChangePercent)s, %(weightedAvgPrice)s, "
                        "%(prevClosePrice)s, %(lastPrice)s, %(lastQty)s, %(bidPrice)s, %(bidQty)s, %(askPrice)s, %(askQty)s, %(openPrice)s, "
                        "%(highPrice)s, %(lowPrice)s, %(volume)s, %(quoteVolume)s, %(openTime)s, %(closeTime)s, %(firstId)s, "
                        "%(lastId)s, %(count)s)")

updateQueryBinanace = ("UPDATE binance SET "
                        "priceChange = %(priceChange)s, priceChangePercent = %(priceChangePercent)s, weightedAvgPrice = %(weightedAvgPrice)s, " 
                        "prevClosePrice = %(prevClosePrice)s, lastPrice = %(lastPrice)s, lastQty = %(lastQty)s, bidPrice =%(bidPrice)s,"
                        "bidQty = %(bidQty)s, askPrice = %(askPrice)s, askQty = %(askQty)s, openPrice = %(openPrice)s, highPrice = %(highPrice)s,  " 
                        "lowPrice = %(lowPrice)s, volume = %(volume)s, quoteVolume = %(quoteVolume)s, openTime = %(openTime)s, "
                        "closeTime = %(closeTime)s , firstId = %(firstId)s, lastId = %(lastId)s, count = %(count)s WHERE symbol = %(symbol)s")

updateQueryBinanace2 = ("UPDATE binance SET "
                        "`priceChange` = %s, `priceChangePercent` = %s, `weightedAvgPrice` = %s, " 
                        "`prevClosePrice` = %s, `lastPrice` = %s, `lastQty` = %s, `bidPrice` =%s, "
                        "`bidQty` = %s, `askPrice` = %s, `askQty` = %s, `openPrice` = %s, `highPrice` = %s,  " 
                        "`lowPrice` = %s, `volume` = %s, `quoteVolume` = %s, `openTime` = %s, "
                        "`closeTime` = %s , `firstId` = %s, `lastId` = %s, `count` = %s WHERE `id` = 1")

insertQueryBitkub = ("INSERT INTO bitkub"
                    "(id, symbol, last, lowestAsk, highestBid, percentChange, baseVolume, quoteVolume, isFrozen, high24hr, low24hr, changes, "
                    "prevClose,	prevOpen) VALUE (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                    )                       

updateQueryBitkub = ("UPDATE bitkub SET"
                    "`last` = %s, `lowestAsk` = %s, `highestBid` = %s, `percentChange` = %s, `baseVolume` = %s,"
                    "`quoteVolume` = %s, `isFrozen` = %s, `high24hr` = %s, `low24hr` = %s, `changes` = %s, "  
                    "`prevClose` = %s, `prevOpen` = %s WHERE `symbol` = %s"
                    )
try:
    connection  = mysql.connector.connect(
    user="root",
    password="",
    host="localhost", 
    database='stockal'
    )
    
except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
            exit()
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
            exit()
        else:
            print(err)
            exit()


cursor = connection.cursor() #define cursor for MySQL Quering

#Fetch Binance Crypto Price
def getBinance():
    try:
        global dataJSON 
        response = requests.get(binanceUrl)                                    
        #print('Binance Response code: '+ str(response.status_code))            
        dataJSON = response.json()

    except requests.exceptions.HTTPError as eHTTPError:                         
        print ("BinanceAPI Http Error:", eHTTPError)
    except requests.exceptions.ConnectionError as eConnectionError:
        print ("BinanceAPI Error Connecting:", eConnectionError)
    except requests.exceptions.Timeout as eTimeout:
        print ("BinanceAPI Timeout Error:", eTimeout)
    except requests.exceptions.RequestException as eRequestException:
        print ("BinanceAPI OOps: Something Else", eRequestException)        


#Fetch Bitkub Crypto Price
def getBitkub():
    try:
        global dictJSON
        response = requests.get(bitkubUrl)                                     #Fetching API with method GET
        #print('Bitkub Response code:'+ str(response.status_code))                   #Getting Response Code 
        #print(response.json())
        dictJSON = response.json()  
        #print(dictJSON)                                                        #Print the result

    except requests.exceptions.HTTPError as eHTTPError:                         
        print ("Http Error:", eHTTPError)
    except requests.exceptions.ConnectionError as eConnectionError:
        print ("Error Connecting:", eConnectionError)
    except requests.exceptions.Timeout as eTimeout:
        print ("Timeout Error:", eTimeout)
    except requests.exceptions.RequestException as eRequestException:
        print ("OOps: Something Else", eRequestException) 

def symbolBitkub():
    try:
        global bitkubsymbolJSON
        response = requests.get(bitkubsymbolUrl)                                     
        data = response.json()['result']
        
        for x in range(len(data)):
            
            bitkubsymbolJSON.append(data[x]['symbol'])    

        #print(bitkubsymbolJSON)
        print('Log: Bitkub Symbol')  
    except requests.exceptions.HTTPError as eHTTPError:                         
        print ("Http Error:", eHTTPError)
    except requests.exceptions.ConnectionError as eConnectionError:
        print ("Error Connecting:", eConnectionError)
    except requests.exceptions.Timeout as eTimeout:
        print ("Timeout Error:", eTimeout)
    except requests.exceptions.RequestException as eRequestException:
        print ("OOps: Something Else", eRequestException) 



def insertBinance():
    try:
        getBinance()
        for x in dataJSON:
            cursor.execute(insertQueryBinanace, x)
            connection.commit()

    except requests.exceptions.HTTPError as eHTTPError:                         
        print ("Http Error:", eHTTPError)
    except requests.exceptions.ConnectionError as eConnectionError:
        print ("Error Connecting:", eConnectionError)
    except requests.exceptions.Timeout as eTimeout:
        print ("Timeout Error:", eTimeout)
    except requests.exceptions.RequestException as eRequestException:
        print ("OOps: Something Else", eRequestException)    

def insertBitkub():
    try:
        getBitkub()
        for x in range(len(bitkubsymbolJSON)):
            symbol = bitkubsymbolJSON[x]
            data = (dictJSON[symbol]['id'], symbol, dictJSON[symbol]['last'], dictJSON[symbol]['lowestAsk'], dictJSON[symbol]['highestBid'],
            dictJSON[symbol]['percentChange'], dictJSON[symbol]['baseVolume'], dictJSON[symbol]['quoteVolume'], dictJSON[symbol]['isFrozen'],
            dictJSON[symbol]['high24hr'], dictJSON[symbol]['low24hr'], dictJSON[symbol]['change'],dictJSON[symbol]['prevClose'],
            dictJSON[symbol]['prevOpen'])
            print(data)
            cursor.execute(insertQueryBitkub, data)
            connection.commit()
        print('Done')    
    except requests.exceptions.HTTPError as eHTTPError:                         
        print ("Http Error:", eHTTPError)
    except requests.exceptions.ConnectionError as eConnectionError:
        print ("Error Connecting:", eConnectionError)
    except requests.exceptions.Timeout as eTimeout:
        print ("Timeout Error:", eTimeout)
    except requests.exceptions.RequestException as eRequestException:
        print ("OOps: Something Else", eRequestException) 



def updateBinance():
    try:
        getBinance()
        
        '''data = (dataJSON[0]['priceChange'], dataJSON[0]['priceChangePercent'], dataJSON[0]['weightedAvgPrice'], dataJSON[0]['prevClosePrice']
        , dataJSON[0]['lastPrice'], dataJSON[0]['lastQty'], dataJSON[0]['bidPrice'], dataJSON[0]['bidQty']
        , dataJSON[0]['askPrice'], dataJSON[0]['askQty'], dataJSON[0]['openPrice'], dataJSON[0]['highPrice']
        , dataJSON[0]['lowPrice'], dataJSON[0]['volume'], dataJSON[0]['quoteVolume'], dataJSON[0]['openTime']
        , dataJSON[0]['closeTime'], dataJSON[0]['firstId'], dataJSON[0]['lastId'], dataJSON[0]['count']
        )'''
        
        for x in dataJSON:
            cursor.execute(updateQueryBinanace, dataJSON[0])
            connection.commit()
        print('Log: Binance Data has been updated')

    except mysql.connector.Error as err:
        print(err)
        #exit()
        
    
def updateBitkub():
    try:
        getBitkub()
        for x in range(len(bitkubsymbolJSON)):
            try:
                symbol = bitkubsymbolJSON[x]
                data = (dictJSON[symbol]['last'], dictJSON[symbol]['lowestAsk'], dictJSON[symbol]['highestBid'], dictJSON[symbol]['percentChange'],
                dictJSON[symbol]['baseVolume'], dictJSON[symbol]['quoteVolume'], dictJSON[symbol]['isFrozen'], dictJSON[symbol]['high24hr'],
                dictJSON[symbol]['low24hr'], dictJSON[symbol]['change'], dictJSON[symbol]['prevClose'], dictJSON[symbol]['prevOpen'], bitkubsymbolJSON[x]
                )
                
                cursor.execute(updateQueryBitkub, data)
                connection.commit()
                
            except KeyError:
                pass    
        print('Log: Bitkub Data has been updated')
    except mysql.connector.Error as err:
        print(err)  
    
    
symbolBitkub()
schedule.every(5).seconds.do(updateBinance)
schedule.every(5).seconds.do(updateBitkub)

while 1:
    schedule.run_pending()
    time.sleep(1)
