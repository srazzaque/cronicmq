(ns cronicmq.concurrency-test
  (:require [cronicmq.core :as cmq]))

(defn pub-thread
  [pub-ctr publisher num-messages]
  (Thread. (fn
             []
             (dotimes [n num-messages]
               (publisher (str "[" (.getName (Thread/currentThread))  "] Sending message no: " n)))
             (swap! pub-ctr inc))))

(defn sub-thread
  [subscription counter])

(comment (deftest pub-sub-roundtrip
           (testing "many publisher threads and one subscriber")
           (let [pub-ctr (atom 0)
                 p (cmq/publisher "tcp://localhost:1234")
                 s (cmq/subscription "tcp://localhost:1234")
                 pub-threads (repeatedly 5 (partial pub-thread pub-ctr p 500))
                 sub-threads (repeatedly 5 (partial sub-thread s counter))]
             (doseq [t pub-threads] (.start t))
             (doseq [t sub-threads (.start t)])
                                        ; Await all threads to complete what they're doing and check counter.
             )
           ))
