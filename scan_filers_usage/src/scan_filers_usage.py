"""
Scan filers.
This python code forms the basis of a parallel scan process for Netapp filers.
This code can be run against each vserver and will start at each qtree on the vserver.
What can be gathered?

File name.
Size of file in bytes
File time of creation.
File time of modification.
and other features.

How could scanning 500+ filers be done in parallel?

Each filer is capable of receiving NMSDK requests.
Each filer KNOWS the files in the qtree on the filer
Each file can be examined in a directory structure through a depth first search
and the time the file was created can be captured as can the modification dates and the size of the file.

How would all the vservers be scanned?

Netbox keeps a list of all the vservers.

The vservers are pulled from the list in Netbox.

A list of qtrees is gathered from the vserver.

The software below scans the qtree and examples ALL the files on the qtree and gathers
time of file creation.
time of file modification.
size of file
number of files per qtree

All of this information is fed into a database from 500+ scans in PARALLEL.


I propose we scan with NMSDK and we scan each filer in parallel to make the
possibility of the scan report happening in a day or two instead of weeks for all of Nvidia filers and vservers.

"""

import pymysql
import sys
import xmltodict
from NaServer import NaServer, NaElement


def setup_cdot(filer):
    s = NaServer(filer, 1, 150)
    s.set_server_type("FILER")
    s.set_transport_type("HTTPS")
    s.set_port(443)
    s.set_style("LOGIN")
    s.set_admin_user("some_username", "some_password")
    #s.set_vfiler("svm11")
    return s

def setup_7mode(filer):
    s = NaServer(filer, 1 , 8)
    s.set_server_type("FILER")
    s.set_transport_type("HTTP")
    s.set_port(80)
    s.set_style("LOGIN")
    s.set_admin_user("admin_user", "admin_password")
    return s


def read_netbox_table():
    con = pymysql.connect( host="localhost", user="username", passwd="password", db="reporting" )
        
    with con.cursor() as cur:
        cur.execute("SELECT name, site, platform from location")
        rows = cur.fetchall() 
    con.close()
    return rows

def file_list_directory_iter(s, path):
    api = NaElement("file-list-directory-iter")
   
    api.child_add_string("max-records","100000000")
    api.child_add_string("path", path)
    
    
    #try:
    xo = s.invoke_elem(api)
    #except:
        #pass
    if (xo.results_status() == "failed") :
        print ("Error:\n")
        print (xo.sprintf())
        sys.exit (1)
    
    print ("Received:\n")
    #print (xo.sprintf())
    return xo.sprintf()
 
def list_aggrs_7mode(s):
    api = NaElement("aggr-list-info")
    #api.child_add_string("aggregate","<aggregate>")
    #api.child_add_string("verbose","<verbose>")
    
    xo = s.invoke_elem(api)
    if (xo.results_status() == "failed") :
        print ("Error:\n")
        print (xo.sprintf())
        sys.exit (1)

    print ("Received:\n")
    #print (xo.sprintf())
    return(xo.sprintf())

def list_aggrs_cdot(s):
    """
    This function will return aggregates from a filer
    """
    api = NaElement("aggr-get-iter")
    xo = s.invoke_elem(api)
    if (xo.results_status() == "failed") :
        print ("Error:\n")
        print (xo.sprintf())
        sys.exit (1)
    return xo.sprintf()


def filer_dirList(s, path):
    myfiles = []
    mytypes = []
    myfilesize = []
    mydict = file_list_directory_iter(s, path)
    results_dict = xmltodict.parse(str(mydict))
    for file_info in results_dict['results']['attributes-list']['file-info']:
        if file_info['name'] != "." and file_info['name'] != "..":
            myfiles.append(file_info['name'])
            mytypes.append(file_info['file-type'])
            myfilesize.append(file_info['file-size'])
    return myfiles, mytypes, myfilesize
    
#myfiles = filer_dirList("/vol/scratch74/scratch.delete_test_jefftest340")
#print (myfiles)
def qtree_list_iter(s):
    api = NaElement("qtree-list-iter")
    xo = s.invoke_elem(api)
    if (xo.results_status() == "failed") :
        print ("Error:\n")
        print (xo.sprintf())
        sys.exit (1)
    print ("Received:\n")
    return xo.sprintf()

def get_qtrees_list_volumes(s):
    mypath_list = []
    qtrees_xml = qtree_list_iter(s)
    results_dict = xmltodict.parse(str(qtrees_xml))
    for qtree_info in results_dict['results']['attributes-list']['qtree-info']:
        #print (qtree_info['qtree'])
        if str(qtree_info['qtree']) != "None":
            mypath_list.append("/vol/"+str(qtree_info['volume'])+"/"+str(qtree_info['qtree']))
    #mypath_list.pop(0)
    return mypath_list

def get_qtrees_list_exports(s):
    myqtree_list = []
    myexport_list = []
    qtrees_xml = qtree_list_iter(s)
    results_dict = xmltodict.parse(str(qtrees_xml))
    for qtree_info in results_dict['results']['attributes-list']['qtree-info']:
        #print (qtree_info['qtree'])
        if str(qtree_info['qtree']) != "None":
            myqtree_list.append(str(qtree_info['qtree']))
            myexport_list.append(str(qtree_info['export-policy']))
    #mypath_list.pop(0)
    return myqtree_list, myexport_list
    


def getDirDFS(s, path):
    """
    Depth first search into a directory.
    NMSDK has been written to act like OS calls to recurse thru the entire filer directory structure on a qtree.
    """
    files_count = 0
    total_file_size = 0
    # Declare an empty stack
    stack = []
    # Add a directory to the need to traverse the stack
    stack.append(path)
    # Traversing conditional execution: the stack is not empty
    while len(stack) > 0:
        # Taken out of the stack to traverse
        tempPath = stack.pop()
        # Get all the subfolders in the path
        dirList, typeList, sizeList  = filer_dirList(s, tempPath)
        #dirList = os.listdir(tempPath)
        # Iterate
        #for filename in dirList:
        #for filename, filetype, myfilesize in zip(dirList, typeList, fileSize):
        for filename, filetype, filesize in zip(dirList, typeList, sizeList):
            #print (filename, filetype, filesize)
        
            #nextPath = os.path.join(tempPath, filename)
            nextPath = str(tempPath) + "/" + str(filename)
            #if os.path.isfile(nextPath):
            if filetype == "file":
                print(nextPath)
                files_count = files_count + 1
                total_file_size = total_file_size + int(filesize)
                print ("_____________________________________________________")
            #elif os.path.isdir(nextPath):
            elif filetype == "directory":
                stack.append(nextPath)
        print ("\n\nRunning total file size = ",str(total_file_size))
        print ("files processed = "+str(files_count))
    return files_count, total_file_size

"""
Put the name of the vserver here
The files on qtrees on the vservrer will be examined.
"""
vserver = "mydc-cdot01-scr"
s = setup_cdot(vserver)

#myxml = qtree_list_iter(s)
#print (myxml)
qtrees, exports = get_qtrees_list_exports(s)
print ("On vserver: ", str(vserver))
for this_qtree, this_export in zip(qtrees, exports):
    print (str(this_qtree)+" has an export called "+str(this_export))
 
exit(0)


"""
Do the scan for each qtree on the filer
after the scan of all the qtreees is finished a summary is printed.
This COULD be put into a database.
"""
myqtrees = get_qtrees_list_volumes(s)
for qtree in myqtrees:
    files_count, total_file_size = getDirDFS(s, qtree)
    print("__________________________________________________________")
    print("summary for qtree = "+str(qtree))
    print("files counted = "+str(files_count))
    print("total bytes of all files = "+str(total_file_size))
    print("__________________________________________________________")



        