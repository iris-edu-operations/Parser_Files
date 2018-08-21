# R script used to collect all of the metadata from the R FDSN Servers. 
#[1] The first for loop run through all signel letter network codes. 
for( i in 1:length(letters)){
    L = toupper(letters[i])
    print(L)
    wget_alpha_string = paste('wget "https://service.iris.edu/fdsnws/station/1/query?net=',L,'&sta=*&loc=*&cha=*&level=response&format=xml&includecomments=true&nodata=404" --output-document=/Users/tronan/Desktop/Projects/SXML_Validator_Test/Full_DB2/FULLDB_',L,'.xml', sep="")
    #cat(wget_alpha_string)
    SXML = system(wget_alpha_string)# ,intern=TRUE)
#[2] The second for loop run through all of the Double alphebetic error codes for loops 1 and 2 work in combination to creat these codes. 
   for( i2 in 1:length(letters)){
     L2 = toupper(letters[i2])
     print(paste(L,L2, sep=""))
     wget_alphaalpha_string = paste('wget "https://service.iris.edu/fdsnws/station/1/query?net=',L,L2,'&sta=*&loc=*&cha=*&level=response&format=xml&includecomments=true&nodata=404" --output-document=/Users/tronan/Desktop/Projects/SXML_Validator_Test/Full_DB2/FULLDB_',L,L2,'.xml', sep="")
     #cat(wget_alphaalpha_string)
     SXML2 = system(wget_alphaalpha_string)# ,intern=TRUE)
   }
#[3] The third loop runs through all of the alpha numeric network combinations. Loops 1 and 3 work in combination to create the codes.   
   for( i3 in 1:9){
    print(paste(L,i3,sep=""))
    wget_alphanum_string = paste('wget "https://service.iris.edu/fdsnws/station/1/query?net=',L,i3,'&sta=*&loc=*&cha=*&level=response&format=xml&includecomments=true&nodata=404" --output-document=/Users/tronan/Desktop/Projects/SXML_Validator_Test/Full_DB2/FULLDB_',L,i3,'.xml', sep="")
    cat(wget_alphanum_string)
    SXML3 = system(wget_alphanum_string)# ,intern=TRUE)
   }
}

# Collect All doulbe temporary numeric networks. Used to collect all of the metadata infromation from the assembeled data bases. 

for( n1 in 1:9){
    for( n2 in 1:9){
        print(paste(n1,n2,sep=""))
        wget_numnum_string = paste('wget "https://service.iris.edu/fdsnws/station/1/query?net=',n1,n2,'*&sta=*&loc=*&cha=*&level=response&format=xml&includecomments=true&nodata=404" --output-  document=/Users/tronan/Desktop/Projects/SXML_Validator_Test/Full_DB2/FULLDB_Assembled',n1,n2,'.xml', sep="")
        SXML3 = system(wget_alphanum_string)# ,intern=TRUE)
    }       
}        
         
         
         
         
