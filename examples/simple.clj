(require '[zeromq-clj.core :as zmq])
(def ctx (zmq/create-context))
(def sub (zmq/create-sub-socket ctx "tcp://127.0.0.1:5858" "hello"))
(def pub (zmq/create-pub-socket ctx "tcp://127.0.0.1:5858"))
(def ex (java.util.concurrent.Executors/newFixedThreadPool 5))
(def received-messages (atom []))
(def disruptor (zmq/on-msg sub
                           (fn [m]
                             (swap! received-messages conj m))
                           ex))
(zmq/publish pub {:event "foo" :haha "bar"} "hello")
(dotimes [n 1000] (zmq/publish pub {:event n :message "Hello there!"} "hello"))



;; Helper stuff
; (import '(com.lmax.disruptor.dsl Disruptor))
; (import '(org.zeromq ZMQ))
