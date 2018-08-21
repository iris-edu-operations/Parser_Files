# This is a scrpit used to parse teh error codes by network. 
library(dplyr)
# Load the error codes. 
data =  read.table('/Users/tronan/Desktop/Projects/SXML_Validator_Test/Results/Stats_ByNetwork_data2.txt', sep=',')
df = data.frame(data[, 2], data[, 4])

# Reorders and sorts the dagta to specific indexesx
df_index = order(data[, 1])
Error = df[df_index,]

# Changes the name of each column
colnames(Error) = c("Code", "Network")

# Counts and summarize the dataframe
Error_summary = data %>% group_by(Error$Network, Error$Code) %>% summarize(count=n())
write.csv(Error_summary, file="/Users/tronan/Desktop/Projects/SXML_Validator_Test/Full_Database_Errors_2018233.csv", quote = FALSE, row.names = FALSE)
# Make a table that compares networks and error codes. 
Net_Error_Tab = table(Error[,2], Error[,1])
Error_Totals  = rowSums(Net_Error_Tab)
Net_Error_Tab_Tot = cbind(Error_Totals, Net_Error_Tab)

#ReOrder the table based on networks that contain the moast error codes. 
tab_index = order(Net_Error_Tab_Tot[,1], decreasing = TRUE)
Ordered_Net_error =Net_Error_Tab_Tot[tab_index, ]

# Create an R latex table using Xtable. 
digits_vect = rep(0, 27)
xtable(Ordered_Net_error, type="h", digits = digits_vect)

# This tbale is completed by moving the output from xtable to a latex document. This document is found at /Users/tronan/Desktop/Projects/SXML_Validator_Test/Full_Database_Stats_tab.tex
