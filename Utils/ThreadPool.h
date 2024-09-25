//
// Created by anonymous on 11/26/23.
//

#ifndef OLVP_THREADPOOL_H
#define OLVP_THREADPOOL_H


#include "../common.h"
#include <condition_variable>
#include <functional>
#include <unordered_map>
#include <future>

using namespace std;

const int TASK_MAX_THRESHHOLD = INT32_MAX; // INT32_MAX;
const int THREAD_MAX_THRESHHOLD = 1024;
const int THREAD_MAX_IDLE_TIME = 60;



enum class PoolMode
{
    MODE_FIXED,
    MODE_CACHED
};


class Thread
{
    int threadName = 0;
public:

    using ThreadFunc = std::function<void(int)>;

    Thread(ThreadFunc func) : func_(func)
            , threadId_((generateId_)++)
    { }
    ~Thread() = default;

    void start()
    {

        std::thread t(func_, threadId_);
        pthread_setname_np(t.native_handle(), ("executor_"+to_string(threadName++)+"_OFF").c_str());
//   threadName++;
        t.detach();
    }


    int getId()const
    {
        return threadId_;
    }

private:
    ThreadFunc func_;
    static int generateId_;

    int threadId_;
};



class ThreadPool
{
public:
    ThreadPool()
            : initThreadSize_(0)
            , taskSize_(0)
            , idleThreadSize_(0)
            , curThreadSize_(0)
            , taskQueMaxThreshHold_(TASK_MAX_THRESHHOLD)
            , threadSizeThreshHold_(THREAD_MAX_THRESHHOLD)
            , poolMode_(PoolMode::MODE_FIXED)
            , isPoolRunning_(false)
    {}
    ~ThreadPool()
    {
        isPoolRunning_ = false;


        std::unique_lock<std::mutex> lock(taskQueMtx_);
        notEmpty_.notify_all();
        exitCond_.wait(lock, [&]()->bool {return threads_.size() == 0; });
    }

    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;


    void setMode(PoolMode mode)
    {
        if (checkRunningState())
            return;
        poolMode_ = mode;
    }


    void setTaskQueMaxThreadHold(int threshhold)
    {
        if (checkRunningState())
            return;
        taskQueMaxThreshHold_ = threshhold;
    }

    void setThreadMaxThreshHold(int threshhold)
    {
        if (checkRunningState())
            return;
        if (poolMode_ == PoolMode::MODE_CACHED)
        {
            threadSizeThreshHold_ = threshhold;
        }
    }


    template<typename Func, typename... Args>
    auto submitTask(Func&& func, Args&&... args) -> std::future<decltype(func(args...))>
    {

        using RType = decltype(func(args...));
        auto task = std::make_shared<std::packaged_task<RType()>>(
                std::bind(std::forward<Func>(func), std::forward<Args>(args)...));
        std::future<RType> result = task->get_future();


        std::unique_lock<std::mutex> lock(taskQueMtx_);

        if (!notFull_.wait_for(lock, std::chrono::seconds(1),
                               [&]()->bool { return taskQue_.size() < (size_t)taskQueMaxThreshHold_; }))
        {

            std::cerr << "task queue is full, submit task fail." << std::endl;
            auto task = std::make_shared<std::packaged_task<RType()>>(
                    []()->RType { return RType(); });
            (*task)();
            return task->get_future();
        }


        taskQue_.emplace([task]() {(*task)(); });
        taskSize_++;


        notEmpty_.notify_all();


        if (poolMode_ == PoolMode::MODE_CACHED
            && taskSize_ > idleThreadSize_
            && curThreadSize_ < threadSizeThreshHold_)
        {
            std::cout << ">>> create new thread..." << std::endl;


            auto ptr = std::make_unique<Thread>(std::bind(&ThreadPool::threadFunc, this, std::placeholders::_1));
            int threadId = ptr->getId();


            threads_.emplace(threadId, std::move(ptr));

            threads_[threadId]->start();

            curThreadSize_++;
            idleThreadSize_++;
        }


        return result;
    }


    void start(int initThreadSize = std::thread::hardware_concurrency())
    {

        isPoolRunning_ = true;


        initThreadSize_ = initThreadSize;
        curThreadSize_ = initThreadSize;


        for (int i = 0; i < initThreadSize_; i++)
        {

            auto ptr = std::make_unique<Thread>(std::bind(&ThreadPool::threadFunc,this, std::placeholders::_1));
            int threadId = ptr->getId();
            threads_.emplace(threadId, std::move(ptr));
            // threads_.emplace_back(std::move(ptr));
        }


        for (int i = 0; i < initThreadSize_; i++)
        {
            threads_[i]->start();
            idleThreadSize_++;
        }
    }

private:

    void threadFunc(int threadid)
    {
        auto lastTime = std::chrono::high_resolution_clock().now();


        for (;;)
        {
            Task task;
            {

                std::unique_lock<std::mutex> lock(taskQueMtx_);


                std::stringstream sin;
                sin << std::this_thread::get_id();

                //    spdlog::info("Tid:"+sin.str()+":got job!");



                while (taskQue_.size() == 0)
                {

                    if (!isPoolRunning_)
                    {
                        threads_.erase(threadid); // std::this_thread::getid()
                        //			std::cout << "threadid:" << std::this_thread::get_id() << " exit!"
                        //				<< std::endl;
                        exitCond_.notify_all();
                        return;
                    }

                    if (poolMode_ == PoolMode::MODE_CACHED)
                    {

                        if (std::cv_status::timeout ==
                            notEmpty_.wait_for(lock, std::chrono::seconds(1)))
                        {
                            auto now = std::chrono::high_resolution_clock().now();
                            auto dur = std::chrono::duration_cast<std::chrono::seconds>(now - lastTime);
                            if (dur.count() >= THREAD_MAX_IDLE_TIME
                                && curThreadSize_ > initThreadSize_)
                            {

                                threads_.erase(threadid); // std::this_thread::getid()
                                curThreadSize_--;
                                idleThreadSize_--;

                                //		std::cout << "threadid:" << std::this_thread::get_id() << " exit!"
                                //		<< std::endl;
                                return;
                            }
                        }
                    }
                    else
                    {

                        notEmpty_.wait(lock);
                    }
                }

                idleThreadSize_--;

                spdlog::debug("Tid:"+sin.str()+":got job!");


                task = taskQue_.front();
                taskQue_.pop();
                taskSize_--;


                if (taskQue_.size() > 0)
                {
                    notEmpty_.notify_all();
                }


                notFull_.notify_all();
            }


            if (task != nullptr)
            {
                task();
            }

            idleThreadSize_++;
            lastTime = std::chrono::high_resolution_clock().now();
        }
    }


    bool checkRunningState() const
    {
        return isPoolRunning_;
    }

private:
    //std::vector<std::unique_ptr<Thread>> threads_;
    std::unordered_map<int, std::unique_ptr<Thread>> threads_;

    int initThreadSize_;
    int threadSizeThreshHold_;
    std::atomic_int curThreadSize_;
    std::atomic_int idleThreadSize_;


    using Task = std::function<void()>;
    std::queue<Task> taskQue_;
    std::atomic_int taskSize_;
    int taskQueMaxThreshHold_;

    std::mutex taskQueMtx_;
    std::condition_variable notFull_;
    std::condition_variable notEmpty_;
    std::condition_variable exitCond_;

    PoolMode poolMode_;
    std::atomic_bool isPoolRunning_;


};





#endif //OLVP_THREADPOOL_H
