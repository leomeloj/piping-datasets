#!/bin/sh

#echo "Running wrapper script"

STARTTIME=$(date -u)

cat < $PWD"/Pipes/kmer_out.pipe" &

#Note that you MUST use java and not scala to run ScalaKMer or you get a Spark error. Reason unknown...
#scala $PWD"/JARS/ScalaKMer.jar" 5 $PWD"/Pipes/trim_out.pipe" > $PWD"/Pipes/kmer_out.pipe" &
java -jar $PWD"/JARS/ScalaKMer.jar" 5 $PWD"/Pipes/trim_out.pipe" > $PWD"/Pipes/kmer_out.pipe" &

#java -jar $PWD"/JARS/trimmomatic-0.36.jar" SE -phred33 $PWD"/Pipes/rdd_out.pipe" $PWD"/Pipes/trim_out.pipe" ILLUMINACLIP:/usr/local/Trimmomatic-0.36/adapters/TruSeq3-SE.fa:2:30:10 LEADING:3 TRAILING:3 SLIDINGWINDOW:4:15 MINLEN:36 &
java -jar $PWD"/JARS/trimmomatic.jar" SE -phred33 $PWD"/Pipes/rdd_out.pipe" $PWD"/Pipes/trim_out.pipe" ILLUMINACLIP:/usr/local/Trimmomatic-0.36/adapters/TruSeq3-SE.fa:2:30:10 LEADING:3 TRAILING:3 SLIDINGWINDOW:4:15 MINLEN:36 &

#cat <&0 > $PWD"/Pipes/rdd_out.pipe"
cat $PWD"/Pipes/rdd_out.pipe"

ENDTIME=$(date -u)
#echo "Start time: "$STARTTIME
#echo "End time: "$ENDTIME