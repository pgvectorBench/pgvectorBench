// Adapted from https://github.com/progschj/ThreadPool
#pragma once

#include <atomic>
#include <condition_variable>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <thread>
#include <vector>

class ThreadPool {
public:
  ThreadPool(size_t);
  template <class F, class... Args>
  auto enqueue(F &&f, Args &&...args)
      -> std::future<typename std::invoke_result<F, Args...>::type>;
  ~ThreadPool();

  void task_complete();

  void wait_all_tasks_finished();

private:
  // need to keep track of threads so we can join them
  std::vector<std::thread> workers;
  // the task queue
  std::queue<std::function<void()>> tasks;

  // synchronization
  std::mutex queue_mutex;
  std::condition_variable condition;
  std::condition_variable condition_finished;
  bool stop;

  std::atomic<size_t> tasks_added{0};
  std::atomic<size_t> tasks_executed{0};
  std::atomic<bool> all_tasks_added;
};

// the constructor just launches some amount of workers
inline ThreadPool::ThreadPool(size_t threads)
    : stop(false), all_tasks_added(false) {
  for (size_t i = 0; i < threads; ++i)
    workers.emplace_back([this] {
      for (;;) {
        std::function<void()> task;

        {
          std::unique_lock<std::mutex> lock(this->queue_mutex);
          this->condition.wait(
              lock, [this] { return this->stop || !this->tasks.empty(); });
          if (this->stop && this->tasks.empty())
            return;
          task = std::move(this->tasks.front());
          this->tasks.pop();
        }

        task();
        task_complete();
      }
    });
}

// add new work item to the pool
template <class F, class... Args>
auto ThreadPool::enqueue(F &&f, Args &&...args)
    -> std::future<typename std::invoke_result<F, Args...>::type> {
  using return_type = typename std::invoke_result<F, Args...>::type;

  auto task = std::make_shared<std::packaged_task<return_type()>>(
      std::bind(std::forward<F>(f), std::forward<Args>(args)...));

  std::future<return_type> res = task->get_future();
  {
    std::unique_lock<std::mutex> lock(queue_mutex);

    // don't allow enqueueing after stopping the pool
    if (stop || all_tasks_added)
      throw std::runtime_error("enqueue on stopped ThreadPool");

    tasks.emplace([task]() { (*task)(); });
    tasks_added.fetch_add(1);
  }
  condition.notify_one();
  return res;
}

// the destructor joins all threads
inline ThreadPool::~ThreadPool() {
  {
    std::unique_lock<std::mutex> lock(queue_mutex);
    stop = true;
  }
  condition.notify_all();
  for (std::thread &worker : workers)
    worker.join();
}

inline void ThreadPool::task_complete() {
  tasks_executed.fetch_add(1);
  if (all_tasks_added.load() && tasks_executed.load() == tasks_added.load()) {
    std::unique_lock<std::mutex> lock(queue_mutex);
    condition_finished.notify_all();
  }
}

inline void ThreadPool::wait_all_tasks_finished() {
  std::unique_lock<std::mutex> lock(queue_mutex);
  all_tasks_added = true;
  condition_finished.wait(lock, [this] {
    return this->tasks_executed.load() == this->tasks_added.load();
  });
}
