import mysql.connector
from mysql.connector import errorcode
import re
import dicttoxml
import pprint
from xml.dom.minidom import parseString
import collections
import logging


class mysql2xml(object):
        ''' mysql2xml
            This package allows you to make multiple calls to database tables - and will group data based on a unique id.
            For example: If you have data with ID as the primary key in a Car, Plane and Boat set of tables....
            d=mysql2xml(usr='tim',db='Car_Plane_Boat')
            transport={}
            d.Read(transport,'Car_Table','ID')
            d.Read(transport,'Plane_Table','ID')
            d.Read(transport,'Boat_Table','ID')
 
            #The data is now all in a nested dictionary.... if you want an XML file then
            xml_str=d.ToXML(d)
        '''

        def __init__(self,usr,db,pwd='',host='',debug=0):
                ''' __init__ needs usr and db.
                    then optionally it can be given pwd(pasword), host, and debug (0 or 1)
                '''
                
                self.cnx = mysql.connector.connect(user=usr, database=db)
                if debug==1:
                   print('Setting Debug')
                   logging.basicConfig(level=logging.DEBUG)
                   self.logger = logging.getLogger(__name__)
                else:
                   logging.basicConfig(level=logging.WARNING)
                   self.logger = logging.getLogger(__name__)
                self.logger.propagate = False


        def Read(self,dic_to_update,table,keyfield):
            ''' Read needs 3 parameters....
                dict - a dictionary that may or may not have data in it
                table - the table in your database
                keyfield - the name of the key field in your table

            A 'select * from <TABLE>' query is executed - and then keyfield is searched for in the columns found.
            the  keyfield value is used as the dictionary key - all columns are then added to a sub-dictionary (nested dictionary)
            '''

            cur = self.cnx.cursor()
            cur.execute(str.format('select * from %s '%(table)))
            res = cur.fetchall()
            index = self.GetPosition(cur, keyfield)
            if index != -1:
                for r in res:
                    key=r[index]
                    if key not in dic_to_update:
                       self.logger.debug("need to add key %s"%(key))
                       dic_to_update[key]={}
                    else:
                       self.logger.debug("Key %s exists"%(key))
                    for idx in range(len(r)):
                        if idx != index:
                            dic_to_update[key][cur.column_names[idx]]=r[idx]           
            else:
                logging.error("Warn: Key %s in table %s was not found"%(keyfield,table))
            return dic_to_update

        def GetPosition(self,cur, key):
            '''GetPosition: Internal function used to find the location index of a column.
            returns -1 if the column key can not be found.
            '''
            columns = cur.column_names
            for idx in range(len(columns)):
                if cur.column_names[idx] == key:
                    return idx
            return -1

        def ToXml(self,d):
            '''ToXml:
            d - a Dictionary - nested or not

            returns a string representing a valid XML file '''

            logging.basicConfig(level=logging.CRITICAL) 
            self.logger= logging.getLogger(__name__)
            xml = dicttoxml.dicttoxml(wpt, custom_root='Data_Load')
            dom = parseString(xml)
            return dom.toprettyxml()


if __name__ == "__main__":
        d=mysql2xml(usr='tim',db='WRC',debug=1)
        wpt={}
        d.Read(wpt,'WptOrig','WptName')
        d.Read(wpt,'WptOrig','WptName')
        #As XML
        print("as XML: %s"%(d.ToXml(wpt)))
        #as pprint
        pprint.pprint(wpt)
