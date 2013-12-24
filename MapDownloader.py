# -*- coding:utf-8 -*-
import Image,os,sys,socket,time,re,urllib2,threading,argparse
from sgmllib import SGMLParser 

class ParseMapURL(SGMLParser):
    def __init__(self):
        SGMLParser.__init__(self)
        self.is_a = ""
        self.mapdict={}
        self.currentkey=""
        
    def start_a(self, attrs):
        self.is_a = 1
        href = [v for k, v in attrs if k=='href']
        #get href of map
        if href:  
            if "http://nrs.harvard.edu/urn-3" in href[0]:
                try:
                    items = re.split(r'\?|:',href[0])
                    if len(items)  > 4:
                        num = items[3]
                        if re.match(r'\d{6}',num) :
                            time.sleep(1)
                            urlopen = urllib2.urlopen(href[0]) 
                            realurl = urlopen.geturl()
                            realitems = re.split(r'\?|/',realurl)
                            self.currentkey = realitems[5]
                            print("..."),
                            sys.stdout.flush()
                            time.sleep(1)
                except:
                    print "XXX" 
                    
    def end_a(self):
        self.is_a = ""
        
    def handle_data(self, text):
        if self.is_a == 1:           
            if re.match(r'\w{2}\d{2}-\d',text):
                #self.mapid.append(text)
                self.mapdict[self.currentkey]=text
                    
class Crawler(object):
    
        def __init__(self,downloadthreadnum,mergethreadnum,timeout,imagequality):
            self.downloadthreadnum = downloadthreadnum
            self.mergethreadnum = mergethreadnum
            self.timeout = timeout
            self.imagequality = imagequality
            self.mapfile = 'maps.txt'
            self.mapindex='http://hcl.harvard.edu/libraries/maps/collections/series_indices/China_Quad_List.html'
            self.mapdict = {}
            self.downloadthreads = []
            self.mergethreads = []
        
        def _getContent(self, url):
                req = urllib2.Request(
                        url = url,
                        headers = {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.4 (KHTML, like Gecko) Chrome/22.0.1229.94 Safari/537.4'}
                )
                self.content = urllib2.urlopen(req).read()

        def start(self):
                #check if exist
                if os.path.isfile(self.mapfile):
                    mapfile = open(self.mapfile,'r')
                    for line in mapfile.readlines():
                        key,value = line.split('\t')
                        self.mapdict[key]=value.strip(' ').strip('\n')
                else:                                         
                    print("Begin to parse maps hidden url ...")
                    
                    # get the content
                    self._getContent(self.mapindex)
                    #parse URL
                    parse=ParseMapURL()                
                    parse.feed(self.content)
                    self.mapdict = parse.mapdict
                    
                    #saved for next time
                    file_object = open('maps.txt', 'w')
                    for key in parse.mapdict:
                        file_object.write(key)
                        file_object.write('\t')
                        file_object.write(parse.mapdict[key])
                        file_object.write('\n')                               
                    file_object.close()
                    print("Parse maps hidden url successfully！")
                
                #download
                for key in self.mapdict:
                    downloadimage = DownloadImage(key,self.mapdict[key],self.timeout)
                    self.downloadthreads.append(downloadimage)
                    downloadimage.start()
                        
                    if len(self.downloadthreads)>=self.downloadthreadnum:
                        #wating for all thread done
                        for t in self.downloadthreads:
                            t.join()
                        #clear thread pool
                        self.downloadthreads =[]
                
                #wating for all thread done
                for t in self.downloadthreads:
                    t.join()
                #clear thread pool
                self.downloadthreads =[]
                                                    
                #merge
                for key in self.mapdict:
                    time.sleep(3)
                    imagemerge = ImageMerge(self.mapdict[key],self.imagequality)
                    imagemerge.start()
                    self.mergethreads.append(imagemerge)
                    
                    if len(self.mergethreads)>=self.mergethreadnum:
                        #wating for all thread done
                        for t in self.mergethreads:
                            t.join()
                        #clear thread pool
                        self.mergethreads = []
                
                #wating for the completion
                for t in self.mergethreads:
                    t.join()
                         
                print("Congratulations，All maps are downloaded and merged！")
                                        
class DownloadImage(threading.Thread): 
    def __init__(self, imageid,imagename,timeout):
        threading.Thread.__init__(self)
        self.imageid = imageid
        self.imagename = imagename
        self.thread_stop = False
        socket.setdefaulttimeout(timeout)
        self.urls = []
        
    def run(self):
        while not self.thread_stop:
            self.download()
        time.sleep(3)
        
    def stop(self):
        self.thread_stop = True
    
    def buildurl(self):
        urlpre = "http://ids.lib.harvard.edu/ids/view/Converter?id="+self.imageid+"&c=jpgnocap&s=1.0000&r=0&"
        urllast ="&w=2400&h=1835"
        for Yindex in range(0,3):
            for Xindex in range(0,3):
                X = Xindex*2400
                Y = Yindex*1835
                urlmid = "x="+X+"&y="+Y
                tempurl = urlpre+urlmid+urllast
                self.urls.append(tempurl)
                
    def download(self):
        strinfo = "Begin to download slices of "+self.imagename+"..."
        print(strinfo)
        #construct map slide url
        urlpre = "http://ids.lib.harvard.edu/ids/view/Converter?id="+self.imageid+"&c=jpgnocap&s=1.0000&r=0&"
        
        for Yindex in range(0,4):
            for Xindex in range(0,4):
                
                imagename = self.imagename + "_"+str(Yindex)+"_"+str(Xindex)+".jpg"
                #check if exists
                if os.path.isfile(imagename):
                    strinfo = imagename+" already exits"
                    print strinfo
                else:                 
                    X = Xindex*2400
                    Y = Yindex*1835
                    urllast ="&w=2400&h=1835"
                    #the last column or last row
                    if Xindex==3:
                        if Yindex==3:
                            w=1646 
                            h=1231
                            urllast ="&w="+str(w)+"&h="+str(h) 
                        else:
                            w =1646                 
                            urllast ="&w="+str(w)+"&h=1835"                    
                    else:
                        if Yindex==3:
                            h=1231
                            urllast ="&w=2400&h="+str(h)                
                        
                    urlmid = "x="+str(X)+"&y="+str(Y)
                    url = urlpre+urlmid+urllast
                
                    imagedata = None
                    while not imagedata:
                        time.sleep(3)
                        socket = urllib2.urlopen(url)
                        time.sleep(5)
                        imagedata = socket.read()
                        
                    try:
                        with open(imagename, "wb") as jpg:
                            jpg.write(imagedata)
                            socket.close()
                            jpg.close()
                            
                            strinfo = imagename+' download successfully' 
                            print(strinfo)
                    except:
                        strinfo = imagename+' download error' 
                        print(strinfo)
        self.stop()
                
class ImageMerge(threading.Thread):
    def __init__(self,imgname,imagequality):
        threading.Thread.__init__(self)
        self.thread_stop = False
        self.imgname = imgname+".jpg"
        self.totalwidth = 8846
        self.totalheight = 6736
        self.imagequality = imagequality
        self.imagepaths =[]
        self.images=[]
        for Yindex in range(0,4):
            for Xindex in range(0,4):
                tempimage = imgname + "_"+str(Yindex)+"_"+str(Xindex)+".jpg"
                self.imagepaths.append(tempimage)
                
    def run(self):
        while not self.thread_stop:
            self.merge()
        time.sleep(3)
        
    def stop(self):
        self.thread_stop = True
                    
    def merge(self):         
        strinfo = "Begin Merge"+self.imgname+"..."
        print(strinfo)
        try :
            mw = 2400
            mh = 1835
            stepX = 0
            stepY = 0

            #check if exists
            if os.path.isfile(self.imgname):
                strinfo = self.imgname+"already exits"
                print strinfo
            else:                     
                for imagepath in self.imagepaths:                
                    image = Image.open(imagepath)
                    self.images.append(image)
          
                target = Image.new('RGB', (self.totalwidth, self.totalheight))
    
                for image in self.images:
                    width, height = image.size            
                    target.paste(image, (0 + stepX, 0 + stepY, width + stepX, height + stepY))
                    stepX += mw
                    if stepX >= self.totalwidth:
                        stepY += mh
                        stepX = 0
                       
                    target.save(self.imgname, quality = self.imagequality)
                       
                    strinfo = self.imgname+'merged successfully'
                    print(strinfo)
        except:
            strinfo = self.imgname+'merged error' 
            print(strinfo)
        self.stop()    
        
if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument("-mappath", help="Set the path of download and merge maps",
                        type=str,default='')
    parser.add_argument("-downloadthread", help="Set the number of download thread, default is 20",
                        type=int,default=20)
    parser.add_argument("-timeout", help="Set socket timeout by second,default is 600",
                    type=int,default=900)  
    parser.add_argument("-mergethread", help="Set the number of merge thread, default is 3. Warning:The map merge operation is resource-intensive,please Make sure your computer's resources sufficient",
                    type=int,default=3)
    parser.add_argument("-imagequality", help="Set the quality of merged map , value from 0 to 100,default is 80",
                    type=int,default=80)  
    args = parser.parse_args()

    statements = "STATEMENT:This tool is only for communication, the COPYRIGHT of data belongs to Harvard Map Collection Digital Maps"
    print ""
    print "Army Map Service Series L500 of China Downloader"
    print "By WeiYoung(新浪微薄：畏着无勇)"
    print ""
    print statements
    print ""
       
    agreement=raw_input('Agree to continue[Y/N]:')
    
    if agreement=='Y' or agreement=='y':        
        if args.mappath!='':
            os.chdir(args.mappath)
        crawler = Crawler(args.downloadthread,args.timeout,args.mergethread,args.imagequality)
        crawler.start()
    else:
        quit()
