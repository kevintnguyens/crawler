import logging
from datamodel.search.datamodel import ProducedLink, OneUnProcessedGroup, robot_manager
from spacetime_local.IApplication import IApplication
from spacetime_local.declarations import Producer, GetterSetter, Getter
from lxml import html,etree
import re, os
from time import time
from bs4 import BeautifulSoup
import urlparse
import cgi
try:
    # For python 2
    from urlparse import urlparse, parse_qs
except ImportError:
    # For python 3
    from urllib.parse import urlparse, parse_qs


logger = logging.getLogger(__name__)
LOG_HEADER = "[CRAWLER]"
url_count = 0 if not os.path.exists("successful_urls.txt") else (len(open("successful_urls.txt").readlines()) - 1)
if url_count < 0:
    url_count = 0
MAX_LINKS_TO_DOWNLOAD = 3000
mostOutboundLinks = ("url", 0)
subdomains = dict()

@Producer(ProducedLink)
@GetterSetter(OneUnProcessedGroup)
class CrawlerFrame(IApplication):

    def __init__(self, frame):
        self.starttime = time()
        # Set app_id <student_id1>_<student_id2>...
        self.app_id = "13942307_78016851_23968158"
        # Set user agent string to IR W17 UnderGrad <student_id1>, <student_id2> ...
        # If Graduate studetn, change the UnderGrad part to Grad.
        #Function 
        self.UserAgentString = 'IR W17 UnderGrad 13942307, 78016851, 23968158'

	# Q
	# Analytics global variables
	try:
            os.remove("analytics.txt")
        except OSError:
            pass
        
        self.invalidLinks = 0
        
        self.frame = frame
        assert(self.UserAgentString != None)
        assert(self.app_id != "")
        if url_count >= MAX_LINKS_TO_DOWNLOAD:
            self.done = True

    def initialize(self):
        self.count = 0
        l = ProducedLink("http://www.ics.uci.edu/", self.UserAgentString)
        print l.full_url
        self.frame.add(l)

    def update(self):
        for g in self.frame.get(OneUnProcessedGroup):
            print "Got a Group"
            outputLinks = process_url_group(g, self.UserAgentString)
            for l in outputLinks:
                if is_valid(l) and robot_manager.Allowed(l, self.UserAgentString):
                    lObj = ProducedLink(l, self.UserAgentString)
                    self.frame.add(lObj)
                else:
                    print l + 'was invalid\n'
                    self.invalidLinks += 1
        if url_count >= MAX_LINKS_TO_DOWNLOAD:
            self.done = True

    # Writes analytics to a file at the end of the crawl
    def writeAnalyticsToFile(self):
        with open('analytics.txt', 'a') as anaFile:
            anaFile.write('Invalid Links: '+str(self.invalidLinks))
            anaFile.write('\nAverage Download Time: '+str(round((time()-self.starttime), 2)/url_count)+' Seconds')
            anaFile.write('\nPage With Most Outbound Links: '+mostOutboundLinks[0]+' with '+str(mostOutboundLinks[1])+' links')
            anaFile.write('\n\nSubdomain Counts:')

            # Print out count of subdomains
            for sub in subdomains:
                subcount = 0
                for path in subdomains[sub]:
                    subcount += subdomains[sub][path]
                anaFile.write('\n'+str(sub)+': '+str(subcount))

    def shutdown(self):
        print "ed ", url_count, " in ", time() - self.starttime, " seconds."
        # calling new analytics functions
        self.writeAnalyticsToFile()
        #print subdomains
        pass

def save_count(urls):
    global url_count
    url_count += len(urls)
    with open("successful_urls.txt", "a") as surls:
        surls.write("\n".join(urls) + "\n")

def process_url_group(group, useragentstr):
    rawDatas, successfull_urls = group.download(useragentstr, is_valid)
    save_count(successfull_urls)
    return extract_next_links(rawDatas)
    
#######################################################################################
'''
STUB FUNCTIONS TO BE FILLED OUT BY THE STUDENT.
'''
def extract_next_links(rawDatas):
    outputLinks = list()
    global mostOutboundLinks
    global subdomains
    '''
    rawDatas is a list of tuples -> [(url1, raw_content1), (url2, raw_content2), ....]
    the return of this function should be a list of urls in their absolute form
    Validation of link via is_valid function is done later (see line 42).
    It is not required to remove duplicates that have already been downloaded. 
    The frontier takes care of that.

    Suggested library: lxml
    '''
    
    #print rawDatas
    #O ( n * k ) k is size of string size
    
    for item in rawDatas:
        tagCount = 0
        soup = BeautifulSoup(item[1], 'lxml')
        for tag in soup.findAll('a',href=True):
            url=strip_anchor(tag['href'])
            parsed = urlparse(tag['href'])
            #if url does begin with http or https. It its an absolute url
            if parsed.scheme in set(["http", "https"]):
                outputLinks.append(tag['href'])
                tagCount += 1
            #rel link with /. replace path with new path
            elif len(tag['href'])>1 and tag['href'][0]=='/' and tag['href'][1]!='/' :
                parsed = urlparse(item[0])
                url=parsed.scheme+parsed.netloc
                outputLinks.append(url+'/'+tag['href'])
                tagCount += 1
            #path is // replace hostname with this
            elif len(tag['href'])>1 and tag['href'][0]=='/' and tag['href'][1]=='/':
                outputLinks.append('http'+tag['href'])
                tagCount+=1
            else
                #append # , ? and everything else to current url
                outputLinks.append(item[0]+'/'+tag['href'])
        # Update for Part 3 in analytics
        if tagCount > mostOutboundLinks[1]:
            mostOutboundLinks = (item[0], tagCount)

        # Track subdomains for analytics
        itemSubdomain = (urlparse(item[0])).hostname
        itemPath = (urlparse(item[0])).path

        if itemSubdomain in subdomains:
            # subdomain exists

            if itemPath in subdomains[itemSubdomain]:
                # path has been crawled, increment count
                subdomains[itemSubdomain][itemPath] += 1

            else:
                subdomains[itemSubdomain][itemPath] = 1
                
        else:
            # subdomain does not exist, create default
            subdomains[itemSubdomain] = {itemPath: 1}
            
    return outputLinks
#given a url. return the query in dictonary
def query_dict(url):
    try:
        url_parse=urlparse(url)
        #found on stackover flow because parse_qs does not work
        return cgi.parse_qs(url_parse.query)
        #return dict(query.split('=') for query in url_parse.query.split("&"))
    except Exception as e:
        print str(e)
        return ''

#check repeating directory. If a directory path is repeated x amount of times or a string. Its a trap
def check_rep(url, x=3):
    parsed=urlparse(url)
    #get rel path
    rel_path=parsed.path
    path_count=dict()
    for path in rel_path.split('/'):
        if path in path_count:
            path_count[path]+=1
            if path_count[path]>x:
                with open('traps.txt', 'a') as anaFile:
                    anaFile.write('traps:'+str(parsed.hostname)+str(parsed.path)+'\t Reapeated path\n')
                return True
        else:
            path_count[path]=1
    return False
        
#given a path. Check if it has been visted before

def check_trap(url, x=5):
    #if the url has been visted x amount of times remove it
    parsed=urlparse(url)
    possible_trap=0
    exceptions=['id','ID','page','PAGE','ucinetid']
    querys=query_dict(url)

    #check if file end in php
    if '.php' in url:
        possible_trap=1
    #if there are query values Then its possibly a trap
    if len(querys)!=0:
        possible_trap=1
    
    if possible_trap:
#        return 1
      if parsed.hostname in subdomains:               
                 
             for exception in exceptions:
                 
                 if exception in querys:
                     return 0
             if parsed.path in subdomains[parsed.hostname]:
                 if subdomains[parsed.hostname][parsed.path] >= x:
                     with open('traps.txt', 'a') as anaFile:
                         anaFile.write('traps:'+str(parsed.hostname)+str(parsed.path)+' Query page was repeated x times\n')

                     return 1
    return 0


def strip_anchor(url):
    return url.split('#')[0]
def is_valid(url):
    '''
    Function returns True or False based on whether the url has to be downloaded or not.
    Robot rules and duplication rules are checked separately.

    This is a great place to filter out crawler traps.
    '''
 
    #given a url break the query. try using beautiful soup. Put this in a hashmap
    #function 2 is check the anchor tag. and strip it from url.
    #return False means url is not used awesome.
    #function 3 get time over count and write it to result.txt as Averge Time: X
    #function 4. is store information in to a dictionary such that it can be transferd to a json file.
    #function 5
    #given a hashmap of a list of urls and query parament. check if it has been hit more then x amount of times. Make exceptions for page query.
    url=strip_anchor(url)
    parsed = urlparse(url)

    #query_dict(url)
    if parsed.scheme not in set(["http", "https"]):
        return False
    if check_trap(url):
        return False
    if check_rep(url):
        return False
    try:
        return ".ics.uci.edu" in parsed.hostname \
            and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4"\
            + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
            + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
            + "|thmx|mso|arff|rtf|jar|csv"\
            + "|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

    except TypeError:
        print ("TypeError for ", parsed)
