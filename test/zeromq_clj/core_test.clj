(ns zeromq-clj.core-test
  (:require [clojure.test :refer :all]
            [zeromq-clj.core :refer :all]
            [zeromq-clj.zmq :as zmq]
            [clojure.core.async :as async]))

(def pub-socket-args (atom []))

(def socket-args (atom []))

(def context-args (atom []))

(def send-args (atom []))

(def subscribe-args (atom []))

(def send-more-args (atom []))

(def recv-args (atom []))

(defn mock-fn
  "Used to capture the value of the call to the mock function."
  ([arg]
     (mock-fn arg "mock-value"))
  ([arg return-val]
     (fn [& args]
       (reset! arg (vec args))
       return-val)))

(defn with-mocks
  [f]
  (doseq [i [pub-socket-args socket-args context-args send-args subscribe-args send-more-args recv-args]]
    (reset! i []))
  (f))

(use-fixtures :each with-mocks)

(deftest test-publishing

  (testing "create publisher with the implicit global context"
    (with-redefs-fn {#'zmq/pub-socket! (mock-fn pub-socket-args)
                     #'zmq/context! (fn [& _] "implicit-context")}
      (fn []
        (is (function? (publisher "tcp://foo-bar-baz:1234")))
        (is (= ["implicit-context" "tcp://foo-bar-baz:1234"] @pub-socket-args)))))

  (testing "create publisher with explicit context"
    (with-redefs-fn {#'zmq/pub-socket! (mock-fn pub-socket-args)}
      (fn []
        (publisher "mock-context" "tcp://foo-bar-baz:1234")
        (is (= ["mock-context" "tcp://foo-bar-baz:1234"] @pub-socket-args)))))

  (testing "send topic and payload on single-topic publisher"
    (with-redefs-fn {#'zmq/pub-socket! (mock-fn pub-socket-args "mock-socket")
                     #'zmq/context! (mock-fn context-args)
                     #'zmq/send-more! (mock-fn send-more-args)
                     #'zmq/send! (mock-fn send-args)}
      #(let [p (publisher "tcp://some-url:1234/someTopic")]
         (is (not (nil? (p "payload"))))
         (is (= ["mock-socket" "someTopic"] @send-more-args))
         (is (= ["mock-socket" "payload"] @send-args)))))

  (testing "send topic and payload on multi-topic publisher"
    (with-redefs-fn {#'zmq/pub-socket! (constantly "mock-socket")
                     #'zmq/send-more! (mock-fn send-more-args)
                     #'zmq/send! (mock-fn send-args)}
      #(let [p (publisher "mock-context" "tcp://foo-bar-baz:1234")]
         (is (p "payload" "topic"))
         (is (= ["mock-socket" "topic"] @send-more-args))
         (is (= ["mock-socket" "payload"] @send-args)))))
  
  (testing "create multi-topic publisher and send payload without topic - should cause an error"
    (with-redefs-fn {#'zmq/pub-socket! (constantly "mock-socket")}
      #(let [p (publisher "mock-context" "tcp://foo-bar-baz:1234")]
         (is (thrown? clojure.lang.ArityException (p "payload")))))))


(deftest test-subscription

  (testing "create topic subscription with implicit context."
    (with-redefs-fn {#'zmq/context! (constantly "the-context")
                     #'zmq/sub-socket! (mock-fn socket-args)
                     #'zmq/subscribe! (mock-fn subscribe-args)}
      (fn []
        (is (subscription "tcp://foo.bar:1234/topicName"))
        (is (= ["the-context" "tcp://foo.bar:1234"] @socket-args))
        (is (= ["mock-subscription" "topicName"]) @subscribe-args))))
  
  (testing "create all-topic subscription."
    (with-redefs-fn {#'zmq/context! (constantly "the-context")
                     #'zmq/sub-socket! (mock-fn socket-args)
                     #'zmq/subscribe! (mock-fn subscribe-args)}
      (fn []
        (is (subscription "tcp://foo.bar:1234/*"))
        (is (= ["the-context" "tcp://foo.bar:1234"] @socket-args))
        (is (= ["mock-subscription" ""]) @subscribe-args)))))

(deftest test-onmsg

  (testing "on-msg should perform the function it was provided with."
    (let [mock-message (promise)
          action-performed (atom false)]
      (with-redefs-fn {#'zmq/recv! (fn [_] @mock-message)}
        (fn []
          (is (on-msg "mock-subsocket"
                      :do (fn [msg] reset! action-performed msg)
                      :while (constantly true)))
          (deliver mock-message "foobar")
          (is "foobar" @action-performed)))))

  (testing "on-msg should continue pulling messages from the socket"
    (let [channel (async/timeout 500)
          processed-messages (atom 0)]
      (with-redefs-fn {#'zmq/recv! (async/<!! channel)}
        (fn [] 
          (is (on-msg "mock-subsocket"
                      :do (fn [_] (swap! processed-messages inc))
                      :while (constantly true)))
          (doseq [n (range 1 100)]
            (async/>!! channel "x")
            (is (= n @processed-messages)))))))

  (testing "on-msg should stop performing the function provided if the :while condition returns false."
    (let [channel (async/timeout 500)
          processed-messages (atom 0)
          process? (atom true)]
      (with-redefs-fn {#'zmq/recv! (async/<!! channel)}
        (fn
          []
          (is (on-msg "mock-subsocket"
                      :do (fn [_] (swap! processed-messages inc))
                      :while (fn [] @process?)))
          (async/>!! channel "x")
          (is (= 1 @processed-messages))
          (reset! process? false)
          (doseq [n (range 1 100)]
            (async/>!! channel "x"))
          (is (= 1 @processed-messages)))))))

(deftest implicit-context

  ; This is the perfect candidate for a property-based test. Given any number of calls to any functions in the namespace,
  ; The zmq/context! should only ever be called once
  (testing "implicit context should only ever be created once"
    (let [call-count (atom 0)]
      (with-redefs-fn {#'zmq/context! (fn [& _] (swap! call-count inc))
                       #'zmq/sub-socket! (mock-fn socket-args)
                       #'zmq/pub-socket! (mock-fn socket-args)}
        (fn [] 
          (subscription "tcp://foo-bar-baz:1412/x")
          (subscription "tcp://foo-bar-baz:1413/x")
          (publisher "tcp://foo-bar-baz:1413/x")
          (publisher "tcp://foo-bar-baz:1413/")
          (is (= 1 @call-count)))))))


(deftest context-cleanup

  (testing "calling close on a context should close all the sockets associated with it."
    (let [closed-items  (atom [])]
      (with-redefs-fn {#'zmq/close! #(swap! closed-items conj %)
                       #'zmq/context! (mock-fn context-args)}
        (fn
          []
          (let [ctx (context)]
            (publisher ctx "publish-addr")
            (subscription ctx "sub-addr")
            (close ctx)
            (is (= ["mock-context" "publish-addr" "sub-addr"] @closed-items)))))))

  (testing "calling close on the implicit context should close all sockets created via the implicit context"
    (let [closed-items  (atom [])]
      (with-redefs-fn {#'zmq/close! #(swap! closed-items conj %)
                       #'zmq/context! (mock-fn context-args "mock-context")}
        (fn
          []
          (let [ctx (context)]
            (publisher "tcp://publish-addr:1234")
            (subscription "tcp://subscribe-address:1234")
            (close)
            (is (= ["mock-context" "publish-addr" "sub-addr"] @closed-items))))))))
