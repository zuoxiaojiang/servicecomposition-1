# ServiceComposition
## BucketServiceComposition
A service composition approach based on the idea of bucket algorithm in traditional data integration.<br>

### Data Structure
##### Underlying relation : 
an instance of Class of  QueryService, by giving parameters : name, output column collection, for example:<br>
val vesseltraj = new QueryService("vesseltraj", Set("mmsi","long","lat","speed"))<br>
##### User query: 
an instance of Class QueryService, by giving parameters : name, underlying relation collection, output column collection,  <br>data constraints, time-window, for example:<br>
val query2 = new QueryService("Q", Set(vesseltravelinfo, vesseltraj,vesselinfo),Set("mmsi","draught",<br> “speed”,"dest"),Set(("speed",40,POSI)),(5,4))<br>
##### service instance: 
with a same form of user query, for example:<br>
val S2 = new QueryService("S2",Set(vesselinfo,vesseltraj),Set("mmsi","callsign","draught","speed"),Set(("speed",30,POSI)),(5,2))<br>
##### service: 
the same form of service instance without giving a parameter of output column collection, for example:<br>
val S3 = new QueryService(“S3”,Set(vesseltraj),Set(("speed",NEGA,30)),(5,2))<br>
### Use Case
#### Data Source
##### underlying relations:
val vesselinfo = <br>
new QueryService("vesselinfo", Set("mmsi","imo","callsign","name","type","length","width","positionType","eta","draught"))<br>
val vesseltraj = new QueryService("vesseltraj", Set("mmsi","long","lat","speed"))<br>
val vesseltravelinfo = new QueryService("vesseltravelinof", Set("imo","dest","source" ))<br>
##### services and service instances
val S10 = new QueryService("S2",Set(vesseltravelinfo), Set( "dest", "source"),Set(("imo",3000,POSI)),(5,2))<br>
val S11 = new QueryService("S3",Set(vesseltraj),Set("mmsi", "speed"),Set(("speed",NEGA,30)),(5,1))<br>
val S12 = new QueryService("S1",Set(vesseltraj,vesselinfo), Set("mmsi", "draught", "speed"),Set(("imo",NEGA,2000)),(5,2))<br>
val S13 = 
new QueryService("S4",Set(vesseltravelinfo,vesselinfo), Set("mmsi", "draught", "dest"), Set(("imo",3000, POSI),("speed",30,POSI)),(5,2))<br>
val S14 = new QueryService("S5",Set(vesseltraj,vesselinfo), Set(("mmsi",1000, POSI)),(5,1))<br>
##### user query:
val query2 = 
new QueryService("Q", Set(vesseltravelinfo, vesseltraj,vesselinfo),Set("mmsi","draught", "speed","dest"),Set(("speed",40,POSI)),(5,4))<br>
#### Case Result
##### print element information in each bucket :
vesseltravelinof's bucket size is :2<br>
Set(S2, S4)<br>
vesseltraj's bucket size is :2<br>
Set(S1, S5)<br>
vesselinfo's bucket size is :3<br>
Set(S1, S4, S5)<br>
##### print executable plans information:
This case found S5 and S4 can be combined as an executable contained rewriting to answer query2, and naturally S5 needs to generate an 
instance for it is a service in composition, so the ops information of S5 is also print to console:<br>
1 --  -- Set(S5, S4) -- Set(mmsi, draught, speed, dest) -- Set((imo,3000,2147483647), (mmsi,1000,2147483647), (speed,30,2147483647)) -- <br>
(5,2)<br>
Include query consists of services :<br>
S5Set(vesseltraj, vesselinfo)Set(name, callsign, imo, draught, long, mmsi, eta, length, type, positionType, width, lat, speed)     <br>
, and ops is : (Set(draught, mmsi, speed),,Set((speed,40,2147483647)),(5,4))     <br>
S4Set(vesseltravelinof, vesselinfo)Set(mmsi, draught, dest)<br>
### Running Program
#### Setting Service Collection
This step occurs in DataUtil.scala, where you can set your own query,  underlying relations and service set with the above data <br>
structure.
First, you need to set the underlying relations below the annotation of “data source”,  then set the service/service instance under the 
annotation “service instances and services”, after that you can propose your queries below the annotation “queries”.<br>
Finally, the variable query needs assigning among your preinstalled queries, and variable services also needs assigning service 
collection. If you wanna some simulated services/service instances as the value of variable services, you could set underlying relations
as value of simulSource and pass it to the method SourceSImulation.genViews(), at the same time the query and the number you wanna generate should also pass to the method.
#### Program Entry
This step occurs in BucketServiceComb.scala, we just need to set the query to variable query:<br>
val query = DataUtil.query<br>
and then set the existed services/service instances like this:<br>
val services = DataUtil.services<br>
or val services = DataUtil.simuServices<br>
the former is the services specified by your self, the latter is the services you simulated.<br>
