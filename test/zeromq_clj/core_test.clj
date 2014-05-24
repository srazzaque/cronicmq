(ns zeromq-clj.core-test
  (:require [clojure.test :refer :all]
            [zeromq-clj.core :refer :all]
            [zeromq-clj.zmq :as zmq]
            [zeromq-clj.serialization :as ser]
            [clojure.core.async :as async]))

(def context-args (atom []))

(def pub-socket-args (atom []))

(def socket-args (atom []))

(def send-args (atom []))

(def subscribe-args (atom []))

(def send-more-args (atom []))

(def recv-args (atom []))

(def has-more-args (atom []))

(def serialize-args (atom []))

(def deserialize-args (atom []))

(defn mock-fn
  "Used to capture the value of the call to the mock function."
  ([arg]
     (mock-fn arg "mock-value"))
  ([arg return-val]
     (fn [& args]
       (reset! arg (vec args))
       return-val)))

(defn mock-fn-echo
  "Mock function with single arg, capture arg and echo what was passed in"
  [arg]
  (fn [incoming-arg]
    (swap! arg conj incoming-arg)
    incoming-arg))

(defn clear-args
  [f]
  (doseq [i [pub-socket-args socket-args send-args subscribe-args
             send-more-args recv-args serialize-args deserialize-args
             context-args]]
    (reset! i []))
  (f))

(use-fixtures :each clear-args)

(deftest test-publishing
  (testing "create publisher"
    (with-redefs-fn {#'zmq/pub-socket! (mock-fn pub-socket-args)}
      (fn []
        (is (function? (publisher "tcp://foo-bar-baz:1234")))
        (is (= ["mock-context" "tcp://foo-bar-baz:1234"] @pub-socket-args)))))
  (testing "send topic and payload on single-topic publisher"
    (with-redefs-fn {#'zmq/pub-socket! (mock-fn pub-socket-args "mock-socket")
                     #'zmq/send-more! (mock-fn send-more-args)
                     #'zmq/send! (mock-fn send-args)
                     #'zmq/context! (mock-fn context-args "mock-context")
                     #'ser/serialize (mock-fn-echo serialize-args)}
      #(let [p (publisher "tcp://some-url:1234/someTopic")]
         (is (not (nil? (p "payload"))))
         (is (= ["mock-socket" "someTopic"] @send-more-args))
         (is (= ["mock-socket" "payload"] @send-args))
         (is (= ["someTopic" "payload"] @serialize-args))))))

(deftest multi-topic-publishing
  (testing "send topic and payload on multi-topic publisher"
    (with-redefs-fn {#'zmq/pub-socket! (constantly "mock-socket")
                     #'zmq/send-more! (mock-fn send-more-args)
                     #'zmq/send! (mock-fn send-args)
                     #'ser/serialize (mock-fn-echo serialize-args)}
      #(let [p (publisher "tcp://foo-bar-baz:1234")]
         (is (p "payload" "topic"))
         (is (= ["mock-socket" "topic"] @send-more-args))
         (is (= ["mock-socket" "payload"] @send-args))
         (is (= ["topic" "payload"] @serialize-args)))))
  (testing "create multi-topic publisher and send payload without topic - should cause an error"
    (with-redefs-fn {#'zmq/pub-socket! (constantly "mock-socket")}
      #(let [p (publisher "tcp://foo-bar-baz:1234")]
         (is (thrown? clojure.lang.ArityException (p "payload")))))))

(deftest test-subscription
  (testing "create subscription."
    (with-redefs-fn {#'zmq/sub-socket! (mock-fn socket-args)
                     #'zmq/subscribe! (mock-fn subscribe-args)
                     #'ser/serialize (mock-fn-echo serialize-args)}
      (fn []
        (is (subscription "tcp://foo.bar:1234/topicName"))
        (is (= ["topicName"] @serialize-args))
        (is (= ["mock-context" "tcp://foo.bar:1234"] @socket-args))
        (is (= ["mock-subscription" "topicName"]) @subscribe-args))))
  (testing "create all-topic subscription."
    (with-redefs-fn {#'zmq/sub-socket! (mock-fn socket-args)
                     #'zmq/subscribe! (mock-fn subscribe-args)}
      (fn []
        (is (subscription "tcp://foo.bar:1234/*"))
        (is (= ["mock-context" "tcp://foo.bar:1234"] @socket-args))
        (is (= ["mock-subscription" ""]) @subscribe-args)))))

(deftest test-pulling-messages-from-subscription
  (testing "should be able to pull messages off the subscription"
    (let [channel (async/to-chan ["topic" "first" "topic" "second"])]
      (with-redefs-fn {#'zmq/sub-socket! (mock-fn socket-args)
                       #'zmq/subscribe! (mock-fn subscribe-args)
                       #'zmq/has-more? (mock-fn has-more-args true)
                       #'zmq/recv! (fn [& _] (async/<!! channel))
                       #'ser/deserialize (mock-fn-echo deserialize-args)}
        (fn
          []
          (let [s (subscription "tcp://foo.bar:1234/topicName")
                first-msg (s)
                second-msg (s)]
            (is (= first-msg "first"))
            (is (= second-msg "second"))
            (is (= ["first" "second"] @deserialize-args))))))))

(deftest cleanup
  (testing "calling close on the implicit context should close all sockets created via the implicit context"
    (let [closed-items (atom [])]
      (with-redefs-fn {#'zmq/close! (fn [x]
                                      (swap! closed-items conj x))
                       #'zmq/pub-socket! (constantly "publish-addr")
                       #'zmq/sub-socket! (constantly "sub-addr")
                       #'zmq/subscribe! (constantly nil)
                       #'zmq/context! (constantly "mock-context")}
        (fn
          []
          (publisher "tcp://publish-addr:1234")
          (subscription "tcp://subscribe-address:1234")
          (close!)
          (is (= ["publish-addr" "sub-addr" "mock-context"] @closed-items)))))))

