(require '[zeromq-clj.core :as zmq])

(require '[clojure.core.async :as a])

(def subscription (zmq/subscription "tcp://127.0.0.1:5858/myTopic"))

(def publisher (zmq/publisher "tcp://127.0.0.1:5858"))

(def received-messages (atom []))

(def done? (promise))

(try
  (let [c (a/chan)]
    (a/go
     (while (not (= 1000
                    (count @received-messages)))
       (swap! received-messages conj (subscription)))
     (deliver done? true))
    (dotimes [n 1000]
      (publisher {:event n :message "Hello there!"} "myTopic")))
  (finally
    @done?
    (zmq/close! :all)))
