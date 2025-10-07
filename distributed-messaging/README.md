IT23656338 - Thewmika W A P          (it23656338@my.sliit.lk)
IT23651210 - Thilakarathna J K D R S (it23651210@my.sliit.lk)
IT23689176 - Tharuka H M N D         (it23689176@my.sliit.lk)
IT23592506 - Karunarathna H W H H    (it23592506@my.sliit.lk)
IT23680470 - G D C Dabarera          (it23680470@my.sliit.lk)



Steps to run

Step 1 :- Run zookeeper
Step 2 :- Open terminal and go to the file location
Step 3 :- Build the project (.\\mvnw.cmd -DskipTests install)
Step 4 :- Start Server
go inside server directory (cd server) and run server (java -jar target\\server-0.0.1-SNAPSHOT.jar --server.port=$PORTNUMBER$ --node.id=$NodeID$)
Step 5 :- Open your browser
Go to: http://localhost:$PORTNUMBER$/
You should see the Distributed Messaging System dashboard
Step 6 :- Add different nodes by running (java -jar target\\server-0.0.1-SNAPSHOT.jar --server.port=$PORTNUMBER$ --node.id=$NodeID$) in diffrent terminals

