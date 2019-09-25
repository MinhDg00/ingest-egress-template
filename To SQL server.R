
library(RODBC)
setwd("Working Directory")
dataset = read.csv("Online.csv")
dataset = as.data.frame(dataset)

driver.name <- "SQL Server"
db.name <- "database name"
host.name <- "host.name"
port <- ""
server.name <- "server name"
user.name <- "username"
pwd <- "password"
# Use a full connection string to connect 
con.text <- paste("DRIVER=", driver.name,
                  ";Database=", db.name,
                  ";Server=", server.name,
                  ";Port=", port,
                  ";PROTOCOL=TCPIP",
                  ";UID=", user.name,
                  ";PWD=", pwd, sep = "")



conn <- odbcDriverConnect(con.text)
odbcClearError(conn)
sqlSave(conn, dataset, tablename = "New Table", rownames=FALSE, safer=FALSE, append=TRUE, fast =F)
close(conn)
