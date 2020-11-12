### BAGORIS Emeline & BOERSMA Hélène - M1 BDML - grp1

# Lab4 - Remarkable trees of Paris

In order to run a mapReduce job, we need an input which is "trees.csv" 
and an output to store the result. The class in charge of the junction 
between path, mapper, reducer, combiner, i/o type, configuration is the 
Job1 class. We just need to adjust the Wordcount example to serve our 
purpose.
The mapper receive the content of the input file line by line. So, we just need to split this line in-between ";" to get each column value. After the map phase, the output of the mapper is input to the reducer. The reducer transforms intermediate data into final data. </br>
Mapper<Kin, Vin, Kout, Vout> </br>
Reducer<Kin, Vin, Kout, Vout>


## <span style="color: #5d9af0"> 1. Districts containing trees (very easy)

This mapReduce job displays the list of districts containing trees and their number by district. The mapper simply output each row district associated with the value 1 except for the fist line with the columns names. Then the reducer automatically group by district the mapper's output. By the way, we added a counter to sum up the number of trees by district when we dis the 7th mapReduce.

*Command*    

    -sh-4.2$ yarn jar hadoop-examples-mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar districtWithTrees /user/ebagoris/trees.csv /user/ebagoris/lab4job1

*Output*

    -sh-4.2$ hdfs dfs -cat lab4job1/*
    11      1
    12      29
    13      2
    14      3
    15      1
    16      36
    17      1
    18      1
    19      6
    20      3   
    3       1
    4       1
    5       2
    6       1
    7       3
    8       5
    9       1


## <span style="color: #5d9af0"> 2. Show all existing species (very easy)

This MapReduce job displays the list of different tree species in this file. We do the exact same thing as before with the "species". Only this time, the reducer just group by species and output the specie name without counting them.

*Command*

    -sh-4.2$ yarn jar hadoop-examples-mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar species /user/ebagoris/trees.csv /user/ebagoris/lab4job2


*output*

    -sh-4.2$ hdfs dfs -cat lab4job2/*
    araucana
    atlantica
    australis
    baccata
    bignonioides
    biloba
    bungeana
    cappadocicum
    carpinifolia
    colurna
    coulteri
    decurrens
    dioicus
    distichum
    excelsior
    fraxinifolia
    giganteum
    giraldii
    glutinosa
    grandiflora
    hippocastanum
    ilex
    involucrata
    japonicum
    kaki
    libanii
    monspessulanum
    nigra
    nigra laricio
    opalus
    orientalis
    papyrifera
    petraea
    pomifera
    pseudoacacia
    sempervirens
    serrata
    stenoptera
    suber
    sylvatica
    tomentosa
    tulipifera
    ulmoides
    virginiana
    x acerifolia

## <span style="color: #5d9af0"> 3. Number of trees by species (easy)

This mapReduce job displays the list of the number of trees by species. The mapper output each row species associated with the value 1. The reducer add the values of the mapper's output group by species.

*Command*

    -sh-4.2$ yarn jar hadoop-examples-mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar nbTrees /user/ebagoris/trees.csv /user/ebagoris/lab4job3

*Output*

    -sh-4.2$ hdfs dfs -cat lab4job3/part-r-00000
    araucana        1
    atlantica       2
    australis       1
    baccata 2
    bignonioides    1
    biloba  5
    bungeana        1
    cappadocicum    1
    carpinifolia    4
    colurna 3
    coulteri        1
    decurrens       1
    dioicus 1
    distichum       3
    excelsior       1
    fraxinifolia    2
    giganteum       5
    giraldii        1
    glutinosa       1
    grandiflora     1
    hippocastanum   3
    ilex    1
    involucrata     1
    japonicum       1
    kaki    2
    libanii 2
    monspessulanum  1
    nigra   3
    nigra laricio   1
    opalus  1
    orientalis      8
    papyrifera      1
    petraea 2
    pomifera        1
    pseudoacacia    1
    sempervirens    1
    serrata 1
    stenoptera      1
    suber   1
    sylvatica       8
    tomentosa       2
    tulipifera      2
    ulmoides        1
    virginiana      2
    x acerifolia    11

## <span style="color: #5d9af0"> 4. Maximum height per specie of tree (average)

This MapReduce job calculates the height of the tallest tree for each specie. This one was a little bit more tricky as we were dealing with missing values. We just kept getting number format problem. In the end, we just had to add a condition to reject empty string in the mapper. The reducer just compute the maximum height of trees by species.

*Command*

    -sh-4.2$ yarn jar hadoop-examples-mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar maxHeight /user/ebagoris/trees.csv /user/ebagoris/lab4job4


*Output*

    -sh-4.2$ hdfs dfs -cat lab4job4/*
    Araucariaceae   9.0
    Betulaceae      20.0
    Bignoniaceae    15.0
    Cannabaceae     16.0
    Cornaceae       12.0
    Cupressaceae    20.0
    Ebenaceae       14.0
    Eucomiaceae     12.0
    Fabaceae        11.0
    Fagaceae        31.0
    Ginkgoaceae     33.0
    Juglandaceae    30.0
    Magnoliaceae    35.0
    Malvaceae       20.0
    Moraceae        13.0
    Oleaceae        30.0
    Paulowniaceae   20.0
    Pinaceae        30.0
    Platanaceae     45.0
    Sapindacaees    12.0
    Sapindaceae     30.0
    Simaroubaceae   35.0
    Taxaceae        13.0
    Taxodiaceae     35.0
    Ulmaceae        30.0

## <span style="color: #5d9af0"> 5. Sort the trees height from smallest to largest (average)

This MapReduce job sorts the trees height from smallest to largest. We just add to repeat the previous mapReduce job but this time, we just passed a NullWritable as value and passed it to the reducer. The output of the mapper is automatically sorted so the reducer just have to group by species and output the name of each one.

*Command*

    yarn jar hadoop-examples-mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar treesSort /user/ebagoris/trees.csv /user/ebagoris/lab4job5

*Output (just the beginning)*

    -sh-4.2$ hdfs dfs -cat lab4job5/*
    2.0
    5.0
    6.0
    9.0
    10.0
    10.0
    10.0
    10.0
    10.0
    11.0
    12.0
    12.0
    12.0
    12.0

    
## <span style="color: #5d9af0"> 6. District containing the oldest tree (difficult)

This MapReduce job displays the district where the oldest tree is. This one was really difficult for us. We first tried to use MapWritable and ArrayWritable in vain. Then we tried to create our own subWritable class. So, we created a subclass called PairWritable that stores a pair of IntWritable. We added the constructors, getters, setters and necessary functions to override. Then we came face to face with another problem : the output key and value of the mapper were different from the reducer. We had (IntWritable, PairWritable) as output type for the mapper and (IntWritable, NullWritable) as output for the reducer. To solve this problem, we deleted the combiner and added setMapOutputKeyClass and setMapOutputValueClass to our Job6 class. ... 


*Command*

    yarn jar hadoop-examples-mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar eldestTreeDistrict /user/ebagoris/trees.csv /user/ebagoris/lab4job6

*Output (The oldest tree is in the 5e district)*

    -sh-4.2$ hdfs dfs -cat lab4job6/*
    5

## <span style="color: #5d9af0"> 7.  District containing the most trees (very difficult)

This MapReduce job displays the district that contains the most trees. The difficulty of this one was to understand how we could run two successive mapReduce. As for the rest, we just had to reuse what we saw precedently. 

So, the main challenge was to correctly link the i/o files of our two steps. We put two jobs side to side into our Job7 class. And we channeled our job this way : </br>
trees.csv -> Job1 -> outputJob1 -> Job2 -> outputJob2 <\br>

Then, we reused the first mapReduce "Districts containing trees" in the first phase in order to output a list of pairs (district, number). 
We correctly connected the output types into the job. 
In phase 2, we reused our PairWritable to output (1, Pair(district, number)) from our second mapper. Then we simply choosed the district with the highest number of trees in the second reducer.

*Command*

    yarn jar hadoop-examples-mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar greenestDistrict /user/ebagoris/trees.csv /user/ebagoris/lab4job7

*Here are the two files that were created :*

    -sh-4.2$ hdfs dfs -ls lab4job7/*
    Found 2 items
    -rw-r--r--   3 ebagoris hdfs          0 2020-11-12 21:56 lab4job7/outputJob1/_SUCCESS
    -rw-r--r--   3 ebagoris hdfs         80 2020-11-12 21:56 lab4job7/outputJob1/part-r-00000
    Found 2 items
    -rw-r--r--   3 ebagoris hdfs          0 2020-11-12 21:57 lab4job7/outputJob2/_SUCCESS
    -rw-r--r--   3 ebagoris hdfs          6 2020-11-12 21:57 lab4job7/outputJob2/part-r-00000


*This is the result of the first mapReduce job :*

    -sh-4.2$ hdfs dfs -cat lab4job7/outputJob1/part-r-00000
    11      1
    12      29
    13      2
    14      3
    15      1
    16      36
    17      1
    18      1
    19      6   
    20      3
    3       1
    4       1
    5       2
    6       1
    7       3
    8       5
    9       1


*Here is the final result where the greenest district is the 16e with 36 trees !*
    -sh-4.2$ hdfs dfs -cat lab4job7/outputJob2/part-r-00000
    16      36
