# PeaceLand Project 

## Situation 
 

### Client 
 

Peaceland is a blessed country, led by an affable and clear-sighted ruler. He takes great pride in his effort to bring peace, happiness, and harmony to all its citizens. To do so, they heavily rely on their peacemakers. A governmental agency dedicated to making peace around the country. To reach their ambition, they bring assistance to any agitated person and help them to recover peace. More generally they help citizen to stay in line with their country's harmonious goal. To help its peacemakers squads, Peaceland engineers have created a working autonomous drone called Peacewatcher. They need you to create the program that will receive and manage 
peacewatchers’s data. This program must: 
- store every peace-watcher data 
- trigger alerts 
- enable peacemaker officers to perform analysis on peace-watcher data 
 

### Drone description 
Each peace-watcher sends a report every minute. 
One report contains  
● peace-watcher id   
● peace-watcher current location (latitude, longitude)   
● name of surrounding citizens (identify with facial recognition) with their 
computed «peace score»   
● words heard by the peace-watcher in its surrounding   
 

### Alert 

 
When a citizen's peace score is bad, your program must trigger an alert with the location of the peacewatcher and the name of the agitated citizen. 
Peacemakers will take it from there and help the person to find peace. 
They may send him to a peace camp. In such a camp citizens learn to reach 
happiness following the ideas of the benevolent leader of Peaceland. Or they will put him in a sustainable and never-ending peace state. 
This alert must be triggered as quickly as possible because an agitated citizen may spread its lack of peace to other citizens. Thus, the peacemaker's reaction must be as fast as possible. 

 
### Statistics 
Peacemakers are convinced that we need to keep every peace-watcher report to make statistics and improve their Peaceland harmony. But they still don’t know what kind of question/statistic they will want to address. 
Peaceland engineers estimate that when the first wave of peace-watchers will be 
operational the sum of all their daily report will weigh 200Gb 
They also estimate that less than 1% of peace-watcher reports contain alerts. 
 

### Failed attempt 
To create a POC of the program, Peaceland hired a team of data scientists and despite all their efforts, this team has not been able to set up a scalable program that can handle the load. 

 

## Preliminary questions 
 

#### 1) What technical/business constraints should the data storage component of the program architecture meet to fulfill the requirement described by the customer in paragraph «Statistics» ? So what kind of component(s) (listed in the lecture) will the architecture need? 
 

The data storage component should :

- be resilient: fault tolerance (need to keep every peace-watcher report) is required (we mustn’t lose any data). Hence RDD (resilient distributed dataset) is convenient in our case
- Concerning the program architecture, 200Gb will be daily stored from the peace-watcher. However, we know one machine can bear at most 1TB (on the machine's disk). In 5 days, the threshold will be reached, making Big Data essential in this use case, using several machines. Horizontal scaling: we make our computation distributed to increase the throughput. 
- allow to process the citizen incoming data quickly and make statistics, which makes the data frame also a relevant choice, providing fast processing (faster than RDD thanks to catalyst optimizer) and aggregation allowing statistics on incoming data. 

We use Spark which is the most efficient distributed framework

The architecture will need distributed software :
- Streaming
- Processing: stream processing is more convenient than batch since the amount is not important (1,4MB/minute) and we need to make analytics piece-by-piece as the data is coming almost real-time processing.
- Storage : NoSQL like MongoDB and datalake like HDFS
  - graph databases could be a good fit since the peacemakers want to analyze the behavior of the surroundings from a particular citizen
  - A column-oriented database is also a good choice since each citizen is defined by specific attributes: id (name, surname), location (GPS), name of the surrounding, and the words heard from the latter.
  - *data lake*: data remains forever 
 

#### 2) What business constraint should the architecture meet to fulfill the requirement described in the paragraph «Alert»? Which component to choose? 
 The requirement described in the alert paragraph meets several business constraints. First of all, we have the <b>Time</b> constraint, we need to be able to trigger the alert as quickly as possible because an agitated citizen may spread its lack of peace to other citizens. Thus, the peacemaker's reaction must be as fast as possible. Secondly, we have the <b>Cost</b> constraint because due to our high price of physical or cloud storage architecture. Thirdly, we have the <b>Reliability</b> constraint, we need to be able to have an important amount of peace watchers who are ready to intervene in case there's a giant sad pandemic virus for example.
 
 

#### 3) What mistake(s) from Peaceland can explain the failed attempt? 

- Peaceland requires a real-time computation of the data retrieved. Besides, the amount is high (if a big data framework was taken, it wasn’t Spark) but not enough to use batch processing and hence is not the most efficient way to process data.
- Big data framework taken: here, Spark is the best choice, maybe the team chose HDFS instead.
They did not consider all the technical and functional parts.  

They could not identify all the variables necessary for the efficient computation.
Also the team of the POC was composed only of data scientists when data engineers were also required.



 
#### 4) Peaceland has likely forgotten some technical information in the report sent by the drone. In the future, this information could help Peaceland make its peace-watchers much more efficient. Which information ? 
-The location of the incident for every citizen, so that the drone can be able to prevent a given citizen is in a place where he was used to cause incidents   
-a concentration of citizens with bad peace score   
-the current and live location of citizens with the worst peace score   
-establish areas of tension ( areas where we notice the most incidents)  
-the date of the incident  

## Project 

 
Peaceland understands this is beyond their team's limits, it can not establish a program to deal with the drone’s data. Peaceland asks you for advice to design an architecture allowing them to create a product they could sell to different police forces. 
It's up to you to report and recommend the right architecture. 
Based on the preliminary questions, your solution is very likely to include :   
● at least one distributed storage   
● at least one distributed stream   
● at least two stream consumer   
