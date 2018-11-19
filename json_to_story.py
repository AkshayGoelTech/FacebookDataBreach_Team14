import getopt
import sys
import json
import hashlib
import os

def hashhex(s):
  """Returns a heximal formated SHA1 hash of the input string."""
  h = hashlib.sha1()
  h.update(s)
  return h.hexdigest()

def processJSON(jsonFile,outputDir,subDir):

    #Make sure there is an output directory
    if not os.path.exists(os.path.join(outputDir,subDir)): os.makedirs(os.path.join(outputDir,subDir))
    
    URL_FILE = open(os.path.join(outputDir,'all_urls.txt'),'w')
    META_FILE = open(os.path.join(outputDir,'url_metadata.txt'),'w')
    
    #go through the JSON and pull each entry into separate line
    lines = []
    for line in open(jsonFile,'r'):
        #lines.append(json.loads(line)) #This line works for doc-per-line cleaned.json
        #Method for unzipping dictionary-of-lists-per-row cluster.json:
        cluster = json.loads(line)
        singlekeys = [key for key in cluster.keys() if not isinstance(cluster[key], list)]
        listkeys = [key for key in cluster.keys() if isinstance(cluster[key], list)]
        entries = len(cluster[listkeys[0]])
        for x in range(entries):
            lines.append({**{key:cluster[key] for key in singlekeys},**{key:cluster[key][x] for key in listkeys}})
    #at this point each line is a json dictionary for each entry in the json file (cluster)
	
   
    for line in lines:
        
        #define here for URL and article text what the JSON dictionary keys are
		#URL is hashed to filename, text is saved in the file
		#Other attributes here can be saved to all_urls.txt associated with URL
        url = line['originalurl']
        cluster = line['clusterid']
        sentences = line['text']
        
        h = hashhex(url.encode())
        
        fileName = os.path.join(outputDir,subDir,h+'.story') #create the. story file version of the article
        FILE = open(fileName,'w')
        FILE.write(sentences)
        FILE.close()
        URL_FILE.write('%s\n' % url)
        META_FILE.write('%s %s\n' % (url,cluster)) #writes a line with format URL cluster# for each .story file
    
    URL_FILE.close()
    META_FILE.close()

if __name__ == '__main__':
        
	print((sys.argv))
	
	try:
	   opts, args = getopt.getopt(sys.argv[1:],"f:h:o:s:")
	except getopt.GetoptError:
		
		print(("opts:"))
		print((opts))
		
		print(('\n'))
		print(("args:"))
		print((args))
		
		print(("Incorrect usage of command line: "))
		print(('python json_to_hash.py -f <file name> -o <output directory> -s <folder name for story files>'))
	   
	  
	   
		sys.exit(2)
	   
	#initialize cmd line variables with default values
	jsonFile = None
	outputDir = None
	subDir = "story_files"

	
	for opt, arg in opts:
		print((opt,'\t',arg))
		if opt == '-h':
		   print(('python json_to_hash.py -f <file name> -o <output directory> -s <folder name for story files>'))
		   sys.exit()
		elif opt in ("-f"):
		   jsonFile = arg
		elif opt in ("-o"):
			outputDir = arg
		elif opt in ("-s"):
			subDir = arg
        

           
	print('\n')
	print("JSON file:",jsonFile)
	print("Output directory:", outputDir)
	print("Story file subdirectory:", os.path.join(outputDir,subDir))
	print('\n')
    
	processJSON(jsonFile,outputDir,subDir)
