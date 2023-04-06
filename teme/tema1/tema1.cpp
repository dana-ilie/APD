#include <iostream>
#include <fstream>
#include <pthread.h>
#include <stdlib.h>
#include <vector>
#include <bits/stdc++.h>
#include "pthread_barrier_mac.h"


struct mapper_arg {
    int id_res_sets;
    int nr_in_files;
    int nr_reducers;
    int nr_mappers;
    std::vector<std::string> input_files;
    std::vector<int> file_is_claimed;
    std::vector<std::vector<std::unordered_set<long>>> res_sets;
    std::vector<int> exp_claimed;
};

pthread_mutex_t claim_file;
pthread_mutex_t add_sets;
pthread_mutex_t claim_exp;
pthread_barrier_t barrier;

bool is_power_of_exp(long number, int exp)
{
    if (number == 1) {
        return true;
    }

    // lower bound
    long low = 1;

    // upper bound
    long high = 1;
    while (pow(high, exp) <= number) {
        high *= 2;
    }

    // binary search
    while (low < high - 1) {
        long mida = (low + high) >> 1;
        long ab = pow(mida, exp);
        if (ab > number) {
            high = mida;
        } else if (ab < number) {
            low = mida;
        } else {
            return true;
        }
    }
    return false;
}

void *f_mapper(void *arg)
{
    mapper_arg *m_arg = (mapper_arg *)arg;
    std::vector<std::unordered_set<long>> mapper_sets (m_arg->nr_reducers + 1);

    for (int i = 0; i < m_arg->nr_in_files; i++) {
        pthread_mutex_lock(&claim_file);
        // if the file is not claimed by a thread
        if (m_arg->file_is_claimed[i] == 0) {
            m_arg->file_is_claimed[i] = 1;
            pthread_mutex_unlock(&claim_file);
            
            // read from in file
            std::ifstream in_file;
            int nr_values;
            long current_value;
            in_file.open(m_arg->input_files[i]);
            in_file >> nr_values;
            
            for (int j = 0; j < nr_values; j++) {
                in_file >> current_value;
                
                for (int exp = 2; exp <= m_arg->nr_reducers + 1; exp++) {
                    // if the current value is a power of exp
                    if (is_power_of_exp(current_value, exp)) {
                        // add the value to the set of numbers that are powers of exp
                        mapper_sets[exp - 2].insert(current_value);
                    }
                }
            }
            // close the input file
            in_file.close();
        } else {
            pthread_mutex_unlock(&claim_file);
        }
    }

    pthread_mutex_lock(&add_sets);
    m_arg->res_sets[m_arg->id_res_sets] = mapper_sets;
    m_arg->id_res_sets++;
    pthread_mutex_unlock(&add_sets);

	pthread_barrier_wait(&barrier);

    pthread_exit(NULL);
}

void *f_reducer(void *arg)
{
	pthread_barrier_wait(&barrier);

    mapper_arg *m_arg = (mapper_arg *)arg;

    for (int exp = 2; exp <= m_arg->nr_reducers + 1; exp++) {
        std::unordered_set<long> reducer_res;
        std::string file_name = "out" + std::to_string(exp) + ".txt";
        pthread_mutex_lock(&claim_exp);
        // if the exponent is not claimed by a reducer
        if (m_arg->exp_claimed[exp - 2] == 0) {
            m_arg->exp_claimed[exp - 2] = m_arg->exp_claimed[exp - 2] + 1;
            pthread_mutex_unlock(&claim_exp);
            // merge all the sets from each mapper for the exp power
            for (int mapper = 0; mapper < m_arg->nr_mappers; mapper++) {
                for(auto& e: m_arg->res_sets.at(mapper).at(exp - 2)) {
                    reducer_res.insert(e);
                }
            }
            // write the number of unique elements to the output file of the reducer
            std::ofstream out_file(file_name);
            out_file << reducer_res.size();
            out_file.close();
        } else {
            pthread_mutex_unlock(&claim_exp);
        }
    }

    pthread_exit(NULL);
}

int main(int argc, char *argv[])
{
    int nr_mappers, nr_reducers, nr_in_files = 0;
    std::ifstream in_file;
    std::vector<std::string> input_files;

    nr_mappers = atoi(argv[1]);
    nr_reducers = atoi(argv[2]);
    in_file.open(argv[3]);

    // init mutex and barrier
    pthread_mutex_init(&claim_file, NULL);
    pthread_mutex_init(&add_sets, NULL);
    pthread_mutex_init(&claim_exp, NULL);
	pthread_barrier_init(&barrier, NULL, nr_mappers + nr_reducers);


    // read all input files names
    if (in_file.is_open()) {
        std::string file;
        in_file >> nr_in_files;
        std::getline(in_file, file);
        while (in_file) {
            std::getline(in_file, file);
            input_files.push_back(file);
        }
    } else {
        std::cout << "Error when reading from file\n";
    }
    in_file.close();

    pthread_t mappers[nr_mappers];
    pthread_t reducers[nr_reducers];
    int r;
    void *status;
    mapper_arg m_arg;
    m_arg.nr_in_files = nr_in_files;
    m_arg.input_files = input_files;
    m_arg.nr_reducers = nr_reducers;
    m_arg.nr_mappers = nr_mappers;
    m_arg.id_res_sets = 0;
    m_arg.res_sets = std::vector<std::vector<std::unordered_set<long>>> (nr_mappers + 1);
    for (int i = 0; i < nr_mappers + 1; i++) {
        m_arg.res_sets[i] = std::vector<std::unordered_set<long>>(nr_reducers + 1);
    }
    m_arg.file_is_claimed = std::vector<int> (nr_in_files + 1, 0);
    m_arg.exp_claimed = std::vector<int> (nr_reducers + 1, 0);

    // create mappers
    for (int i = 0; i < nr_mappers; i++) {
        r = pthread_create(&mappers[i], NULL, f_mapper, &m_arg);

        if (r) {
            std::cout << "Error when creating mapper thread\n";
            exit(-1);
        }
    }
    // create reducers
    for (int i = 0; i < nr_reducers; i++) {
        r = pthread_create(&reducers[i], NULL, f_reducer, &m_arg);

        if (r) {
            std::cout << "Error when creating reducer thread\n";
            exit(-1);
        }
    }

    // mappers join
    for (int i = 0; i < nr_mappers; i++) {
        r = pthread_join(mappers[i], &status);

        if (r) {
            std::cout << "Error when waiting for mapper thread\n";
            exit(-1);
        }
    }
    // reducers join
    for (int i = 0; i < nr_reducers; i++) {
        r = pthread_join(reducers[i], &status);

        if (r) {
            std::cout << "Error when waiting for reducer thread\n";
            exit(-1);
        }
    }

    // destroy mutex and barrier
    pthread_mutex_destroy(&claim_file);
    pthread_mutex_destroy(&add_sets);
    pthread_mutex_destroy(&claim_exp);
	pthread_barrier_destroy(&barrier);

    pthread_exit(NULL);
}
