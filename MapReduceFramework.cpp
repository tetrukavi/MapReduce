//
// Created by Avi && Liron on 01/06/2021.
//

#include "MapReduceFramework.h"
#include <pthread.h>
#include <vector>
#include <atomic>
#include <algorithm>
#include <set>
#include <map>
#include <semaphore.h>
#include <iostream>
#include "Barrier.h"


/// structs:
typedef struct {
    std::atomic<uint32_t> counter_processed;
//    std::atomic<uint32_t>* counter_all_elements;

    std::atomic<uint32_t> mapping_counter;

//    std::vector<ThreadContext*> threads_context;
    JobState jobState;
    Barrier barrier;

    MapReduceClient &client;
    const InputVec *inputVec;
    OutputVec *outputVec;

    // mutexes & more..

} JobContext;

typedef struct {
    int threadID;
    // std::vector<std::pair<void *, void *>>k_v_vector;
    IntermediateVec* intermediateVec;
    JobContext* jobContext;

} ThreadContext;


/// global fields:
std::vector<ThreadContext*> all_threads_context;


/// functions:
/***
 * This function does the 'reduce' phase.
 * @param threadContext represents the context of the thread
 */
void do_reduce_phase(ThreadContext *threadContext)
{
    threadContext->jobContext->jobState.stage = REDUCE_STAGE;
}

/***
 * This function does the 'shuffle' phase
 * @param threadContext represents the context of the thread
 */
std::vector<IntermediateVec> do_shuffle_phase(ThreadContext *threadContext)
{
    threadContext->jobContext->jobState.stage = SHUFFLE_STAGE;
	std::atomic<uint32_t>* number_of_vectors = nullptr;
	std::map<K2*, IntermediateVec> inter_map;
	for (auto thread_context :all_threads_context)
	{
		for (auto inter_pair:*thread_context->intermediateVec )
		{
			if (inter_map.find(inter_pair.first) == inter_map.end())
			{
				IntermediateVec vec_of_key;
				inter_map.insert({inter_pair.first, vec_of_key});
				number_of_vectors++;
			}
			else
			{
				inter_map[inter_pair.first].push_back(inter_pair);
			}
		}
		thread_context->intermediateVec->clear();


	}
	std::vector<IntermediateVec> result;
	result.reserve(*number_of_vectors);
	for (const auto& k_v: inter_map)
	{
		result.push_back(k_v.second);
	}
	return result;




//
//	int max_len_inter_vec = 70;
//	int ind = 0;
//
//	while(ind < max_len_inter_vec){
//		for (auto thread_context :all_threads_context)
//		{
//			IntermediatePair temp;
//			if (!thread_context->intermediateVec->empty()){
//				temp = thread_context->intermediateVec->back();
//				if (keys_set) { //todo - check if in set
//					result.at(3).push_back(temp);
//				}
//				else{
//
//				}
//			}
//		}
//		ind++;
//	}
}

/***
 * This function does the 'map' phase
 * @param threadContext represents the context of the thread
 */
void do_map_phase(ThreadContext *threadContext)
{
    threadContext->jobContext->jobState.stage = MAP_STAGE;

    int vec_input_size = threadContext->jobContext->inputVec->size();
    while ( (unsigned int) threadContext->jobContext->mapping_counter < vec_input_size){
//        threadContext->jobContext->mapping_counter.fetch_add(1);
		std::cout << (unsigned int) threadContext->jobContext->mapping_counter.load() << std::endl;
		int y = 0 ;

		std::cout << threadContext->jobContext->inputVec->at
				((unsigned int)threadContext->jobContext->mapping_counter).first << std::endl;
		std::cout << threadContext->jobContext->inputVec->at
				((unsigned int)threadContext->jobContext->mapping_counter).second << std::endl;
        threadContext->jobContext->client.map(
                threadContext->jobContext->inputVec->at
                ((unsigned int)threadContext->jobContext->mapping_counter).first,
                threadContext->jobContext->inputVec->at
                ((unsigned int)threadContext->jobContext->mapping_counter).second, threadContext);
    }
}

/***
 * This function is the function that the thread runs - the framework.
 * @param arg represents threadContext
 */
void* foo(void* arg)
{
    auto* threadContext = (ThreadContext *) arg;
    // Map Phase
    do_map_phase(threadContext);
    // Sort Phase
    std::sort(threadContext->intermediateVec->begin(), threadContext->intermediateVec->end());
    // Barrier Phase
    threadContext->jobContext->barrier.barrier();


	sem_t mySemaphore;
	if (sem_init(&mySemaphore, 0, 1) < 0)
	{
		// TODO FAILURE
	}
	sem_wait(&mySemaphore);


	// Shuffle Phase
    std::vector<IntermediateVec> result;
    if (threadContext->threadID == 0)
    {
		result = do_shuffle_phase(threadContext);
    }
	sem_post(&mySemaphore);

	// Reduce Phase
    do_reduce_phase(threadContext);

	if (sem_destroy(&mySemaphore) < 0)
	{
		// TODO FAILURE

	}

	return nullptr;
}

/***
 * This function produces a (K2*, V2*) pair. It receives as input intermediary element (K2, V2) and context
 * which contains data structure of the thread that created the intermediary element. The function saves the
 * intermediary element in the context data structures. In addition, the function updates the number of intermediary
 * elements using atomic counter.
 * @param key represents a pointer to K2
 * @param value represents a pointer to V2
 * @param context represents a threadContext
 */
void emit2 (K2* key, V2* value, void* context){
    // TODO -  UPDATE THE ATOMIC COUNTER
    auto* threadContext = (ThreadContext *) context;
    threadContext->intermediateVec->push_back({key,value});//todo - needs to create pair of intermediate or not??
    threadContext->jobContext->mapping_counter++;
}

/***
 * This function produces a (K3*, V3*) pair. It receives as input output element (K3, V3) and context which contains
 * data structure of the thread that created the output element. The function saves the output element in the context
 * data structures (output vector). In addition, the function updates the number of output elements using atomic counter
 * @param key represents a pointer to K3
 * @param value represents a pointer to V2
 * @param context represents a threadContext
 */
void emit3 (K3* key, V3* value, void* context)
{
	}

/***
 * This function inits the new JobContext
 * @param context represents a JobContext
 * @param client represents a MapReduceClient
 * @param vector represents an input vector
 * @param vector1 represents an output vector
 * @param level represents the number of all the threads
 */
void init_Job_context(JobContext *context, const MapReduceClient &client, const InputVec &vector, OutputVec &vector1,
                      int level)
{
//    context->jobState.stage = UNDEFINED_STAGE;
//    context->jobState.percentage = 0.0;
//    context->client = client;
//    context->inputVec = vector;
//    context->outputVec = vector1;
//    context->barrier = Barrier(level);
//    context->mapping_counter = 0;
}

/***
 * This function inits the new ThreadContext
 * @param threadContext represents a threadContext
 * @param i represents the id of the created thread
 * @param intermediateVec the intermediateVec of the created thread
 * @param jobContext represents a jobContext
 */
void init_thread_context(ThreadContext *threadContext, int i, IntermediateVec *intermediateVec, JobContext *jobContext)
{
    threadContext->threadID = i;
    threadContext->intermediateVec = intermediateVec;
    threadContext->jobContext = jobContext;
}


/***
 * This function starts running the MapReduce algorithm (with several threads) and returns a JobHandle.
 * @param client The implementation of MapReduceClient or in other words the task that the framework should run
 * @param inputVec a vector of type std::vector<std::pair<K1*, V1*>>, the input elements.
 * @param outputVec a vector of type std::vector<std::pair<K3*, V3*>>, to which the output elements will be added
 * before returning. You can assume that outputVec is empty.
 * @param multiThreadLevel the number of worker threads to be used for running the algorithm.
 * @return JobHandle that will be used for monitoring the job
 */
JobHandle startMapReduceJob(const MapReduceClient& client, const InputVec &inputVec, OutputVec&
outputVec,
							int multiThreadLevel)
{
    JobContext *jobContext; // todo - check what to do with it
//    init_Job_context(jobContext, client, inputVec, outputVec, multiThreadLevel);
	jobContext->jobState.stage = UNDEFINED_STAGE;
	jobContext->jobState.percentage = 0.0;
	jobContext->client = client;
	jobContext->inputVec =   &inputVec;
	jobContext->outputVec = &outputVec;
	jobContext->barrier = Barrier(multiThreadLevel);
	jobContext->mapping_counter = 0;

    pthread_t threads[multiThreadLevel];
//	ThreadContext contexts[multiThreadLevel];
	for (int i = 0; i < multiThreadLevel ; ++i)
	{
//		contexts[i] = {i};
        auto threadContext = new ThreadContext;
        auto intermediateVec = new IntermediateVec; // todo - remember to free its memory!!
        init_thread_context(threadContext, i, intermediateVec, jobContext);
        all_threads_context.push_back(threadContext);
//        threadContext = {i};
	}
	for (int i = 0; i < multiThreadLevel; ++i)
	{
		pthread_create(threads + i, nullptr, foo, all_threads_context[i]);
	}

	// todo- may be error?
    return jobContext;
}

/***
 * This function gets JobHandle returned by startMapReduceFramework and waits until it is finished.
 * @param job represents a JobHandle returned by startMapReduceFramework
 */
void waitForJob(JobHandle job)
{}

/***
 * – This function gets a JobHandle and updates the state of the job into the given JobState struct
 * @param job represents a JobHandle
 * @param state the state of the job
 */
void getJobState(JobHandle job, JobState* state){
//    auto jobContext = (JobContext) job;
//    // stage of state
//    state->stage = jobContext.jobState.stage;
//    // percentage of state
    //todo - fill this
}

/***
 * This function releases all resources of a job. You should prevent releasing resources before the job finished.
 * After this function is called the job handle will be invalid. In case that the function is called and the job is
 * not finished yet wait until the job is finished to close it.
 * @param job represents a JobHandle
 */
void closeJobHandle(JobHandle job){
    //  waiting for the job to be finished
    waitForJob(job);
    // releasing all resources of a job
    //todo - fill this
}