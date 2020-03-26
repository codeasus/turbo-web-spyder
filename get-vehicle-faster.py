import csv
import time
import random
import requests
import threading
import concurrent.futures
from bs4 import BeautifulSoup

def write(csvfile, carsPropertyList) :
    csvThreadList = []
    writeRow      = lambda csvFile, row : csvFile.writerow(row)
    with concurrent.futures.ThreadPoolExecutor() as executor :
        try :
            for row in carsPropertyList :
                csvThreadList.append(executor.submit(writeRow, csvfile, row))

        except Exception as fileError :
            print(fileError)

def propertylinkHandler(parentThread, linkToScrap, httpHeader) :
    tempRequest                = requests.get(linkToScrap, headers=httpHeader)
    if tempRequest.status_code == 200 :
        carPropertyList        = []
        carSourceCode          = BeautifulSoup(tempRequest.text, "html.parser")
        carPropertyDiv         = carSourceCode.find_all("div", {'class':'product-properties-value'})
        carStatisticsDiv       = carSourceCode.find('div', {'class' : 'product-statistics'}).find_all('p')
    
        carPropertyList = [carProperty.text for carProperty in carPropertyDiv]

        carPropertyList.append(carStatisticsDiv[0].text.split(' ')[-1])
        carPropertyList.append('-'.join(carStatisticsDiv[1].text.split(' ')[1:]))
        carPropertyList.append(linkToScrap)
        
        if carPropertyList[14] != 'Kreditdədir' :
            carPropertyList.insert(14, 'null')
        if carPropertyList[15] != 'Barter mümkündür' :
            carPropertyList.insert(15, 'null')
        # print("{0} ::: {1} ::: [ FETCHED-CAR-PROPERTIES[ {2} ] ]".format(parentThread, threading.current_thread().name, len(carPropertyList)))
        return carPropertyList
    else :
        print("{0} ::: {1} ::: [ ERROR[ STATUS-CODE-{2} ] ]".format(parentThread, threading.current_thread().name, tempRequest.status_code))
        return None

def carFrameCrawler(parentThread, carFrame, httpHeader) :
    carLinkSolver    = lambda car: "{}{}".format("https://turbo.az", car.find('a')['href'])
    carPropertyList  = []
    carsThreadList   = []
    with concurrent.futures.ThreadPoolExecutor(thread_name_prefix="CAR-THREAD") as executor :
        for car in carFrame : 
            carsThreadList.append(executor.submit(propertylinkHandler, parentThread, carLinkSolver(car), httpHeader))

        for thread in carsThreadList :
            carPropertyList.append(thread.result())
    
    return carPropertyList

def mainCrawler(csvFile, startPage, endPage) :
    httpHeader         = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:73.0) Gecko/20100101 Firefox/73.0'}
    rootLink           = 'https://turbo.az/autos?page='
    totalFetchedCarsPerThread = 0

    while(startPage <= endPage) :
        fetchedCars    = 0
        startTime      = time.perf_counter()
        time.sleep(random.randint(3, 8))
        mainUrlLink    = "{}{}".format(rootLink, startPage)
        mainUrlRequest = requests.get(mainUrlLink, headers = httpHeader)

        if mainUrlRequest.status_code == 200 :
            mainHtmlCode     = BeautifulSoup(mainUrlRequest.text , "html.parser")
            carFrame         = mainHtmlCode.find_all("div", {"class" : "products"})

            carsPropertyList = carFrameCrawler("[ {0} ]".format(threading.current_thread().name), carFrame[2], httpHeader)
            write(csvFile, carsPropertyList)

            fetchedCars               = len(carsPropertyList)
            totalFetchedCarsPerThread = totalFetchedCarsPerThread + fetchedCars
            timeDiff                  = time.perf_counter() - startTime
            print("{0} ::: PAGE[ {1} ] ::: FETCHED-CARS[ {2} ] ::: TIME-SPENT[ {3:.3f} ]".format(threading.current_thread().name, startPage, fetchedCars, timeDiff))
            startPage                 = startPage + 1

        else :
            print("[ ERROR ][ CONNECTION ] ::: [ CAR-FRAME ] ::: [ PAGE[ {} ] ] ::: [ {} ]".format(startPage, mainUrlRequest.status_code))
            break

    return totalFetchedCarsPerThread
        
if __name__ == "__main__":
    fieldNames      = ['Şəhər','Marka','Model','Buraxılış ili','Ban növü','Rəng',
                              'Mühərrikin həcmi','Mühərrikin gücü','Yanacaq növü','Yürüş','Sürətlər qutusu',
                              'Ötürücü','Yeni','Qiymət', 'Kredit', 'Barter', 'Baxışların sayı', 'Yeniləndi', 'Link']
    outputFile      = open(r"vehicle.csv", mode="w", encoding='utf-8', newline='')

    totalCarCollector        = 0
    carFrameThreadsContainer = []
    startTime                = time.perf_counter()
    csvFile                  = csv.writer(outputFile)
    csvFile.writerow(fieldNames)

    with concurrent.futures.ThreadPoolExecutor(thread_name_prefix = "CAR-FRAME-THREAD") as executor :
        lastPage        = 10 # if you wanna give a single thread a page, then set lastPage and startPage to 1 and pageCount to 1
        startPage       = 1 # if you wanna give a single thread a range of page, then set startPage to the min number in the range and lastPage to the max number in the range
        pageCount       = 10 # and the pageCount to the max number in range. For example, when you want each thread to handle 5 page, the min value is 1 and the max is 5.
        dedicatedThread = 100 # pageCount takes care of next range of pages.
                               
        for thread in range(dedicatedThread) : # 0 -> 1,1; 1 -> 2,2; 2 -> 3,3; 3 -> 4,4; 4 -> 5, 5
            carFrameThreadsContainer.append(executor.submit(mainCrawler, csvFile, startPage, lastPage))
            startPage   = startPage + pageCount 
            lastPage    = lastPage  + pageCount
        
        for thread in concurrent.futures.as_completed(carFrameThreadsContainer) :
            totalCarCollector = totalCarCollector + thread.result()

    outputFile.close()
    totalTime = time.perf_counter() - startTime

    print('\nFINISHED ===> T_SECOND(S)[ {0:.3f} ] ::: T_MINUTE(S)[ {1:.3f} ] ::: T_HOUR(S)[ {2:.3f} ] ::: TOTAL-FETCHED-CAR(S)[ {3} ]'.format( totalTime,
                                                                                                                                       totalTime/60, totalTime/3600, totalCarCollector))
