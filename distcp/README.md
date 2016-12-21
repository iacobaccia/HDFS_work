# DISTCP Using HDFS Snapshots
==============================

Distcp is a great tool to move data across clusters using a Mapreduce Job(Mappers Only). More documentation about distcp can be found in the below link:

[Distcp Apache Documentation](https://hadoop.apache.org/docs/r1.2.1/distcp2.html)

It is very straight forward to use distcp to transfer data from one hadoop cluster to another hadoop cluster. The purpose of this script though is to use a new methodology called **HDFS Snapshots** to do the actual distcp.

#### What are HDFS snapshots ?

HDFS Snapshots are read-only point-in-time copies of the file system. Snapshots can be taken on a subtree of the file system or the entire file system. Some common use cases of snapshots are data backup, protection against user errors and disaster recovery.

[Documentation on HDFS Snapshots](https://hadoop.apache.org/docs/r2.7.2/hadoop-project-dist/hadoop-hdfs/HdfsSnapshots.html)


#### Why do we have to use HDFS Snapshots in distcp to transfer data across cluster ?

1. When we use -update option with distcp, the utility takes a namenode listing of the source and target directories and does comparision which is a costly operation and effects the performance for large directories.
2. Ideally, when the number of files in the directories being compared is huge, distcp puts a lot of pressure on Namenodes and in some scenarios, it might cause Namenodes to flip or die.
3. *HDFS Snapshot* is a metadata image of the directory and when we use snapshots to do the distcp, the file comparision is done outside the scope of Namenode which is very efficient.

#### What does the dataMover.sh script do ?

1. The script abstracts the users from implementing the distcp commands using snapshots by themselves and also does maintenance on the existing snapshots(Delete or Do Not Delete).
2. User has to just call the script with the necessary parameters and the script takes care of generating the distcp commands and execute them.
3. The script currently supports two types of refresh:

  * FULL refresh
  * INCREMENTAL refresh
4. The FULL refresh, cleans up any existing snapshots in the target directory and deletes the contents of the target directory and then doest the distcp.
5. *FULL refresh* implementation is not based on HDFS snapshots because we dont have to compare the file listings of source and target directories.
6. *Incremental refresh* implementation is based on HDFS snapshots because it does compare the file listings on source and target directories.
7. The script takes care of creating snapshots in source directory before the disctcp is run and in target directory after distcp is run.
8. You can also pass on options to clean up the snapshots in source and target directories.



#### Pre-requisites:

1. Its better if the source cluster is aware of the target cluster. Please refer to the below Hortonworks HCC post:

[ Name service awareness across clusters ](https://community.hortonworks.com/questions/8989/how-to-use-name-service-id-between-to-clusters.html)

2. As alternative approach, you can use the Active namenode names instead of cluster name service ids as parameters for the script. Haven't tested this approach though.
3. The source directory needs to be snapshshottable
`hdfs dfsadmin -allowSnapshot <hdfs://<name service id of the Source cluster>:<8020>/<HDFS Source directory name>
Example: hdfs dfsadmin -allowSnapshot hdfs://srccluster:8020/dir1/dir2`



#### How to use the script

`./dataMover.sh <Option#1> <Option#2> <Option#3> <Option#4> <Option#5> <Option#6> <Option#7> <Option#8> <Option#9>`

Where:

1. Option#1 - Source dataset name
2. Option#2 - Target dataset name
3. Option#3 - Source cluster Name service id or Active namenode name of source cluster
4. Option#4 - Target cluster Name service id or Active namenode name of target cluster
5. Option#5 - full/incremental      (Choose either full refresh or incremental refresh)
6. Option#6 - delete-source-snapshots/donot-delete-source-snapshots    (delete-source-snapshots will delete the older snapshots in the source directory except the last but one snapshot. donot-delete-source-snapshots option will leave the source directory snapshots itact WITHOUT deleteing them)
7. Option#7 - delete-target-snapshots/donot-delete-target-snapshots    (delete-source-snapshots will delete the older snapshots in the source directory except the last but one snapshot. donot-delete-source-snapshots option will leave the source directory snapshots itact WITHOUT deleteing them)
8. Option#8 - Email address that you want to be notified when you do the FULL refresh and when a HDFS allowSnapshot is needed
9. Option#9 - Number of Mappers incase if you want to control the mappers spun by the distcp job. If the value is not specified, 20 mappers are assumed by default.(Note: distcp works little differently than regular mapreduce in case of coming up with number of mappers.)


`example:  /.dataMover.sh /user/hrongali/source /user/hrongali/target srccluster drcluster full donot-delete-source-snapshots donot-delete-target-snapshots hrongali@xxxxx.com 30`



#####Note:
When you want to run the script for the first time to do the FULL refresh, you will have to make sure the source directory is __snapshottable__. otherwise the subsequent incremental refresh will fail.
The below command needs HDFS super user previliges to run

`hdfs dfsadmin -allowSnapshot <hdfs://<name service id of the Target cluster>:<8020>/<HDFS Target directory name>
Example: hdfs dfsadmin -allowSnapshot hdfs://drcluster:8020/dir1/dir2`
