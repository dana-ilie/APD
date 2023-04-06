# About
Ilie Dana Maria, 334CA
November 2022
-------------------------------------------------------------------------------

# Implementation
## Main
In the main function, the name of each file that needs to be proccesed is read
from the input file and stored in a vector in order to be passed to each thread.
Then, the mappers and reducers are started together. In order to avoid using
global variables, each thread receives as an argument a structure in which are
placed all the parameters needed in the thread function.
In the end, all the mappers and reducers are joined together.
For the synchronisation between mappers and reducers, a barrier of 
(number of mappers + number of reducers) threads was used. The barrier was
placed at the end of the mapper function and at the beginning of the reducer
function.

-------------------------------------------------------------------------------

## Mappers
Each mapper performs the following operations for each file it is responsible
for:
    -> opens the file and reads all the values
    -> for each read value, the mapper checks whether it is a perfect power
       or not.
    -> if the value is a perfect power of k, it is inserted in the mapper's set
       for all the values which are powers of k.
    -> In order to avoid keeping duplicates of the values, an unordered_set was
       used in the implementation
    -> closes the file after all the values have been processed

For the distribution of the files to the mappers a vector was used in order
to keep track if a certain file was claimed by a mapper. Each mapper checks if
a file was already claimed. If not, the mapper claims the file and procceses it.
To avoid having multiple mappers claiming the same file, a mutex was used.

In order to determine whether a value X is a perfect power, it is checked if
X is a power of each E in the range 2->number of reducers + 1. The function
"is_power_of_exp" checks this using binary search to improve the performance

-------------------------------------------------------------------------------

## Reducers
Each reducer performs the following operations:
    -> Combines all the sets received from each mapper for the exponent E it is
       responsible for
    -> Counts de unique values in the merged list and writes the result to its
       output file.
