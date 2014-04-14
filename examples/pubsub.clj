(require '[zeromq-clj.core :as zmq] :reload-all)

(do 
  (def ctx (zmq/create-context))
  (def sub (zmq/sub-socket ctx "tcp://127.0.0.1:5858" "myTopic"))
  (def pub (zmq/pub-socket ctx "tcp://127.0.0.1:5858"))
  (def ex (java.util.concurrent.Executors/newFixedThreadPool 5))
  (def received-messages (atom [])))

(def disruptor (zmq/on-msg sub
                           (fn [m]
                             (swap! received-messages conj m))
                           ex))

(try
  (dotimes [n 1000]
    (zmq/publish pub {:event n :message "Hello there!"} "myTopic"))
  (finally
    (.shutdownNow ex)
    (.shutdown disruptor)
    (zmq/close ctx)))
