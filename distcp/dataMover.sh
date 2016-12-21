#!/bin/bash

# This script requires the below parameters:
#      Dataset Name to be replicated
#      Source cluster name
#      Target Cluster name
#      full/incremental
#      source snapdhot activity - Delete or Donot Delete 
#      Target snapshot activity - Delete or Donot Delete
#      email address to be notified in case a -allowSnapshot is needed on the HDFS directory
#      Number of Mappers
    

source_dataset=$1
target_dataset=$2
source_cluster=$3
target_cluster=$4
type_of_refresh=$5
source_snapshots=$6
target_snapshots=$7
email_address=$8
num_of_mappers=$9


# FUNCTION CALL FOR FULL REFRESH

full_refresh()
{
	echo "This is a full refresh $1 to  $2"
	echo " "
	echo "Making sure the source directory is snapshottable..."
	echo " "
	snp_shot_dir_src=$1'.snapshot'

	#echo "Snapshot directory is "$snp_shot_dir

# CHECK IF THE SOURCE DIRECTORY IS SNAPSHOTTABLE. THROW ERROR IF NOT
	check=`hdfs dfs -test -d $snp_shot_dir_src && echo 1 || echo 0`
	#echo $check

	if [ $check == 1 ]; then
        	echo "Snapshot Directory Exists. Proceeding"
		echo " "
	else
        	echo "File $snp_shot_dir_src doesnt exist in Source cluster $source_cluster. Please make the source directory $1 snapshottable..."
        	exit
	fi

# CHECK IF YOU WANT TO DELETE THE OLD SNAPSHOTS FROM THE SOURCE DIRECTORY.
	if [ "$3" == "delete-source-snapshots" ]; then
		echo "Deleting previous snapshots from Source directory...."
		echo " "
		latest_snapshot_src=` hdfs dfs -ls -t $snp_shot_dir_src | grep "/" | sed -e 's/ \+/|/g' | cut -d "|" -f8 | rev | cut -d "/" -f1 | rev`

		for snapshot in $latest_snapshot_src ; do
                        echo "Deleting $snapshot"
                        delete_snapshot="hdfs dfs -deleteSnapshot $1 $snapshot"
                        echo $delete_snapshot
                        echo " "
                        $delete_snapshot
                done
		
	fi

# GET THE LATEST SNAPSHOT FROM THE SOURCE DIRECTORY TO BUMP UP THE NEXT SNAPSHOT VERSION AND CREATE SNAPSHOT WITH THE BUMPED VERSION
	no_of_snapshot_files=`hdfs dfs -ls $snp_shot_dir_src | grep "/" |wc -l`

	if [ "$no_of_snapshot_files" -gt 0  ]; then
		echo "Snapshots exist in the snapshot directory"
		echo " "

        	latest_snapshot_src=` hdfs dfs -ls -t $snp_shot_dir_src | grep "/" | head -n1 | sed -e 's/ \+/|/g' | cut -d "|" -f8 | rev | cut -d "/" -f1 | rev`
        	echo "Latest snapshot dir is "$latest_snapshot_src" incrementing the snapshot version..."
		echo " "

        	latest_version_src=${latest_snapshot_src:1:999}
        	echo "Latest snapshot version is $latest_version_src"
		echo " "

        	incremented_snapshot_version=$((latest_version_src+1))

        	incremented_snapshot_filename="s"$incremented_snapshot_version

        	echo "Incremented snapshot file name is $incremented_snapshot_filename"
		echo " "
        	src_snpshot_command="hdfs dfs -createSnapshot $1 $incremented_snapshot_filename"

        	echo $src_snpshot_command
		echo " "
		$src_snpshot_command
	else
		src_snapshot_cmd="hdfs dfs -createSnapshot $1 s1"
		echo $src_snapshot_cmd
		$src_snapshot_cmd
	fi
		

# SINCE THIS IS A FULL REFRESH, TARGET DIRECTORY CAN BE REFRESHED TOO. DELETE THE EXISTING SNAPSHOTS BEFORE CLEANING UP THE DIRECTORY.

	echo "Removing the Target directory $2 and related snapshots"
	echo " "
	snp_shot_dir_tgt=$2'.snapshot'
	
	check=`hdfs dfs -test -d $snp_shot_dir_tgt && echo 1 || echo 0`

        if [ $check == 1 ]; then
                echo "Snapshot Directory Exists on Target. Proceeding"
		latest_snapshot_tgt=` hdfs dfs -ls -t $snp_shot_dir_tgt | grep "/" | sed -e 's/ \+/|/g' | cut -d "|" -f8 | rev | cut -d "/" -f1 | rev`
		
		for snapshot in $latest_snapshot_tgt ; do
			echo "Deleting $snapshot"
			delete_snapshot="hdfs dfs -deleteSnapshot $2 $snapshot"
			echo $delete_snapshot
			echo " "
			$delete_snapshot
		done
        else
                echo "File $snp_shot_dir_tgt doesnt exist in Target cluster $target_cluster. Proceeding...."
		echo " "
        fi

# REMOVING THE TARGET HDFS DIRECTORY
	remove_tgt_dir_command="hdfs dfs -rm -R $2"
	echo $remove_tgt_dir_command
	echo " "
	$remove_tgt_dir_command

# ACTUAL DISTCP PROCESS

	no_of_snapshot_files=`hdfs dfs -ls $snp_shot_dir_src | grep "/" |wc -l`
	echo "Number of snapshot files are $no_of_snapshot_files"
	hdfs dfs -ls $snp_shot_dir_src
	hdfs dfs -ls $snp_shot_dir_tgt

	echo " "

	if [ "$no_of_snapshot_files" -gt 1 ]; then
		
		#distcp_cmd="hadoop distcp $1.snapshot/$incremented_snapshot_filename $2"
		distcp_cmd="hadoop distcp -m $num_of_mappers $1 $2"
		echo $distcp_cmd
		echo " "
		$distcp_cmd
  		
		snp_shot_dir_tgt=$2'.snapshot'

                #echo "Snapshot directory is "$snp_shot_dir

                check=`hdfs dfs -test -d $snp_shot_dir_tgt && echo 1 || echo 0`
                #echo $check

                if [ $check == 1 ]; then
                        echo "Snapshot Directory Exists for the Target Directory. Proceeding"
                        echo " "
                else
                        echo "File $snp_shot_dir_tgt doesnt exist in Source cluster $target_cluster. Please make the target directory $2 snapshttable..."
			echo "HDFS Snapshot needed for the directory  $2 in Target cluster " | mutt -s "HDFS Snapshot is needed for the directory" -- $5
                fi


	else
		#distcp_cmd="hadoop distcp $1.snapshot/s1 $2"
		distcp_cmd="hadoop distcp -m $num_of_mappers $1 $2"
		echo $distcp_cmd
		echo " "
		$distcp_cmd

        	snp_shot_dir_tgt=$2'.snapshot'

        	#echo "Snapshot directory is "$snp_shot_dir

        	check=`hdfs dfs -test -d $snp_shot_dir_tgt && echo 1 || echo 0`
        	#echo $check

        	if [ $check == 1 ]; then
                	echo "Snapshot Directory Exists for the Target Directory. Proceeding"
                	echo " "
        	else
                	echo "File $snp_shot_dir_tgt doesnt exist in Source cluster $target_cluster. Please make the source directory $2 snapshottable..."
			
			echo "HDFS Snapshot needed for the directory  $2 in Target cluster " | mutt -s "HDFS Snapshot is needed for the directory" -- $5
        	fi
	fi

	
}

# FUNCTION FOR INCREMENTAL REFRESH.

incremental_refresh()
{
	echo "This is incremental refresh"

	echo " "

        echo "Making sure the source directory is snapshottable..."
	echo " "

        snp_shot_dir_src=$1'.snapshot'

        #echo "Snapshot directory is "$snp_shot_dir

# CHECK OF THE SOURCE DIRECTORY IS SNAPSHOTTABLE
        check=`hdfs dfs -test -d $snp_shot_dir_src && echo 1 || echo 0`
        #echo $check

        if [ $check == 1 ]; then
                echo "Snapshot Directory Exists. Proceeding"
		echo " "
        else
                echo "File $snp_shot_dir_src doesnt exist in Source cluster $source_cluster. Please make the source directory $1 snapshottable..."
                exit
        fi

# CHECK IF TARGET DIRECTORY EXISTS AT ALL. IF NOT, THIS SHOULD REDIRECT TO FULL REFRESH PATH

	check=`hdfs dfs -test -d $2 && echo 1 || echo 0`

	if [ $check == 1 ]; then
		echo "Target directory exists, Proceeding...."
		echo " "
	else
		echo "Target directory doesnt exist, falling back to full refresh path"
# FULL REFRESH FUNCTION CALL FROM INCREMENTAL REFRESH PATH
		full_refresh $1 $2 $3
	
		exit
		
	fi

# CHECK OF TARGET DIRECTORY IS SNAPSHOTTABLE. IF YES, WORKS ON GETTING THE LATEST AND FIRST SNAPSHOT VERSIONS AND ACTS ACCORDING TO SNAPSHOT MAINTENANCE OPTIONS (DELETE/DONOT DELETE). FINALLY CREATES THE SNAPSHOTS WITH VERSIONS BUMPED AND DOES FULL DISTCP/ DIFF DISTCP.

        snp_shot_dir_tgt=$2'.snapshot'

        #echo "Snapshot directory is "$snp_shot_dir_tgt

        check=`hdfs dfs -test -d $snp_shot_dir_tgt && echo 1 || echo 0`
        #echo $check

        if [ $check == 1 ]; then
                echo "Snapshot Directory Exists for the Target dataset. Proceeding"
                echo " "

        	no_of_snapshot_files=`hdfs dfs -ls $snp_shot_dir_tgt | grep "/" |wc -l`
		echo "Value of no_of_snapshot_files is $no_of_snapshot_files"
		echo " "

		if [ "$no_of_snapshot_files" -eq 0 ];then
			echo "Making sure there is at least one snapshot for the target directory"
			take_snapshot="hdfs dfs -createSnapshot $2 s1"
			echo $take_snapshot
			$take_snapshot

			echo " "
			latest_snapshot_tgt="s1"
		else
			latest_snapshot_tgt=`hdfs dfs -ls -t $snp_shot_dir_tgt | grep "/" | head -n1 | sed -e 's/ \+/|/g' | cut -d "|" -f8 | rev | cut -d "/" -f1 | rev`
		
			first_snapshot_tgt=`hdfs dfs -ls $snp_shot_dir_tgt | grep "/" | head -n1 | sed -e 's/ \+/|/g' | cut -d "|" -f8 | rev | cut -d "/" -f1 | rev`	
			latest_version_tgt=${latest_snapshot_tgt:1:999}
			first_version_tgt=${first_snapshot_tgt:1:999}

	                echo "Latest snapshot version is $latest_version_tgt"
        	        echo " "
			echo "First snapshot version is $first_version_tgt"
			echo " "

                	incremented_snapshot_version=$((latest_version_tgt+1))

                	decremented_snapshot_version=$((latest_version_tgt-1))
			
			if [ "$4" == "delete-target-snapshots" ];then
                        	if [ "$decremented_snapshot_version" -eq 0 ]; then
                                	echo "Only one snapshot existing.... Not deleting the snapshot"
                        	else
                                	echo "Deleting upto the prior latest snapshot because this is incremental refresh we need atleast one snapshot to calculate the diff...."
                                	for i in `seq $first_version_tgt $decremented_snapshot_version`;
                                	do
                                        	snapshot_delete_cmd="hdfs dfs -deleteSnapshot $2 s$i"
                                        	echo $snapshot_delete_cmd
                                        	echo " "
                                        	$snapshot_delete_cmd
                                	done
                        	fi
			fi
			
		fi
			echo "Latest snapshot for Target is $latest_snapshot_tgt"
			echo " "
        else
                echo "File $snp_shot_dir_tgt doesnt exist in Source cluster $target_cluster. Please make the source directory $2 snapshottable..
."
                exit
        fi




        no_of_snapshot_files=`hdfs dfs -ls $snp_shot_dir_src | grep "/" |wc -l`

        if [ "$no_of_snapshot_files" -gt 0  ]; then
                echo "Snapshots exist in the snapshot directory"
		echo " "

                latest_snapshot_src=` hdfs dfs -ls -t $snp_shot_dir_src | grep "/" | head -n1 | sed -e 's/ \+/|/g' | cut -d "|" -f8 | rev | cut -d "/" -f1 | rev`
		first_snapshot_src=`hdfs dfs -ls $snp_shot_dir_src | grep "/" | head -n1 | sed -e 's/ \+/|/g' | cut -d "|" -f8 | rev | cut -d "/" -f1 | rev`

                echo "Latest snapshot dir is "$latest_snapshot_src" incrementing the snapshot version..."
		echo " "

                latest_version_src=${latest_snapshot_src:1:999}
                first_version_src=${first_snapshot_src:1:999}
                
		echo "Latest snapshot version is $latest_version_src"
		echo " "
		echo "First snapshot version is $first_version_src"
		echo " "

                incremented_snapshot_version=$((latest_version_src+1))

		decremented_snapshot_version=$((latest_version_src-1))


		if [ "$3" == "delete-source-snapshots" ]; then

			if [ "$decremented_snapshot_version" -eq 0 ]; then
				echo "Only one snapshot existing.... Not deleting the snapshot"
			else
				echo "Deleting upto the prior latest snapshot because this is incremental refresh we need atleast one snapshot to cqalculate the diff...."
				for i in `seq $first_version_src $decremented_snapshot_version`;
				do
					snapshot_delete_cmd="hdfs dfs -deleteSnapshot $1 s$i"
					echo $snapshot_delete_cmd
					echo " "
					$snapshot_delete_cmd
				done
			fi
		fi

                incremented_snapshot_filename="s"$incremented_snapshot_version

                echo "Incremented snapshot file name is $incremented_snapshot_filename"

                src_snpshot_command="hdfs dfs -createSnapshot $1 $incremented_snapshot_filename"

                echo $src_snpshot_command
		echo " "
		$src_snpshot_command

		distcp_cmd="hadoop distcp -m $num_of_mappers -diff $latest_snapshot_tgt $incremented_snapshot_filename -update $1 $2"
		
		echo $distcp_cmd
		echo " "
		$distcp_cmd
		
		latest_tgt_snapshot_version=${latest_snapshot_tgt:1:999}
                increment_latest_tgt_snapshot_version=$((latest_tgt_snapshot_version+1))

		snapshot_cmd_on_tgt="hdfs dfs -createSnapshot $2 s$increment_latest_tgt_snapshot_version"
		echo $snapshot_cmd_on_tgt
		$snapshot_cmd_on_tgt

        else
                snapshot_cmd="hdfs dfs -createSnapshot $1 s1"
                echo $snapshot_cmd
		echo " "
		$snapshot_cmd
	
		distcp_cmd="hadoop distcp -m $num_of_mappers $1.snapshot/s1 $2"

		echo $distcp_cmd
		echo " "
		$distcp_cmd

		latest_tgt_snapshot_version=${latest_snapshot_tgt:1:999}
                increment_latest_tgt_snapshot_version=$((latest_tgt_snapshot_version+1))

                snapshot_cmd_on_tgt="hdfs dfs -createSnapshot $2 s$increment_latest_tgt_snapshot_version"
        fi


}

# Build the source and destination paths

check_dir_ending_with_slash=`echo $source_dataset  | awk '{print substr($0,length,1)}'`
#echo "check dir ending with slash is " $check_dir_ending_with_slash

if [ "$check_dir_ending_with_slash" != "/" ]; then
	#echo "Not equal to /"
	source_dataset=$source_dataset'/'
fi

check_dir_ending_with_slash=`echo $target_dataset  | awk '{print substr($0,length,1)}'`
#echo "check dir ending with slash is " $check_dir_ending_with_slash

if [ "$check_dir_ending_with_slash" != "/" ]; then
        #echo "Not equal to /"
        target_dataset=$target_dataset'/'
fi

 
source_path='hdfs:''//'$source_cluster':8020'$source_dataset
target_path='hdfs:''//'$target_cluster':8020'$target_dataset

echo $source_path


# Check if the source path exists in the source cluster
check=`hdfs dfs -test -d $source_path && echo 1 || echo 0`
#echo $check

if [ $check == 1 ]; then
	echo "File Exists"
else
	echo "File $source_dataset doesnt exist in Source cluster $source_cluster"
	exit
fi

# Check of the source path is snapshottable
src_snapshot_path=$source_path




# Check if the destination path exists on the target cluster 
check=`hdfs dfs -test -d $target_path && echo 1 || echo 0`
#echo $check

if [ $check == 1 ]; then
        echo "Target File Exists"
else
        echo "File $target_dataset doesnt exist in Target cluster $target_cluster. Distcp will create one"
fi


# Check if the type of refresh is a valid option
if [ "$type_of_refresh" == "full" ] || [ "$type_of_refresh" == "incremental" ]; then
	echo "Valid options"
else
	echo "Invalid type of refresh option!!! Exiting the script...."
	exit
fi

# Check the source_snapshots value
if [ "$source_snapshots" == "delete-source-snapshots" ] || [ "$source_snapshots" == "donot-delete-source-snapshots" ]; then
	echo "Valid source_snapshots value"
	echo " "
else
	echo "Invalid source_snapshots value!!! Please choose either delete-source-snapshots or donot-delete-source-snapshots as option!!!!"
	echo " "
	exit
fi

# Check the target_snapshots value
if [  "$target_snapshots" == "delete-target-snapshots" ] || [ "$target_snapshots" == "donot-delete-target-snapshots" ]; then
	echo "Valid target_snapshots value"
	echo " "
else
	echo "Invalid target_snapshots value !!! Please choose either delete-target-snapshots or donot-delete-target-snapshots as option!!!!"
	echo " "
	exit
fi

if [ "$target_snapshots" == "donot-delete-target-snapshots" ] && [ "$type_of_refresh" == "full" ]; then
	echo "Snapshots in Target directory have to be cleaned up for full refresh. Please choose delete-target-snapshots as an option "
	echo " "
	exit
fi

if [[ $num_of_mappers =~ ^-?[0-9]+$ ]]; then
	echo "Number of mappers is a Valid Number"
else
	echo "Did not enter a valid number for the number of mappers $num_of_mappers. Defaulting, the number of mappers to 20..."
	num_of_mappers=20
fi


# Actual distcp build process

if [ "$type_of_refresh" == "full" ];then
	full_refresh $source_path $target_path $source_snapshots $target_snapshots $email_address
else
	incremental_refresh $source_path $target_path $source_snapshots $target_snapshots $email_address
fi
