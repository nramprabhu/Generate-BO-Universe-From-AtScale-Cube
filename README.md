
This Java program takes the .tds file generated from the AtScale engine after the cube publish and generate a BusinessObjects universe. 

How To Run : 

Please note that , This program can be compiled / run only in a machine where either BO Server or BO Client Tools installation are present. 

1. Copy all the files & Directories  
2. Edit the generateUinverse.bat file 
3. update the -Dbusinessobjects.connectivity.directory to your connectionServer folder , Usually if you have a BO installation the connection server directory would be in , \SAP BusinessObjects Enterprise XI 40\dataAccess\connectionServer 
4. The first parameter is BO CMS name:port eg BO42:6400  
5. The second parameter is BO CMS username  
6. The third parameter is BO CMS Password   
7. After the third parameter, Please specify all the tds files you want to convert into universe . If the tds file name has space in it , then please specify it within double quote. eg DOTA.tds "Internet Sales Cube - Test1.tds" myServicesDrill.tds "Sales Cube.tds"  
8. Please make sure to copy all the .tds files into the root folder (along with the Jar file) mentioned in step v.  

High Level Tasks done by this program :

1. Read the .tds file.
2. Establish a connection to BusinessObjects Server to obtain a valid session.
3. Create a JDBC connection to the AtScale cube. 
4. Create a BusinessObjects Data Foundation Layer from the above connection and save it in the local path provided.
5. Create a BusinessObjects Business Layer and save it in the local path provided. 
6. Add all Dimensions and Measures in folders. Add the folders to the Business Layer. 
7. Publish the connection to BusinessObjects Enterprise Repository. 
8. Publish the Universe ( Connection + Data Foundation + Business Layer) tot he BusinessObjects Enterprise Repository. 

Known limitations : 

1. Ordering of Folders and Objects (Measures & Dimensions) inside folders are alphabetical only. 
2. Number formats & Date Formats are not set from cube. They are defaulted to "none" 
3. To Run the is program we need BusinessObjects supplied Jar files ( sl_sdk.jar , bosdk.jar) and a run time compile parameter pointing to BusinessObjecss connection Server directory.


