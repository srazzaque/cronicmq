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

(defn with-mock-functions
  [f]
  (doseq [i [pub-socket-args socket-args send-args subscribe-args
             send-more-args recv-args serialize-args deserialize-args
             context-args]]
    (reset! i []))
  (binding [*context* (atom nil)]
    (with-redefs-fn {#'zmq/context! (mock-fn context-args "mock-context")
                     #'zmq/pub-socket! (mock-fn pub-socket-args "mock-publisher")
                     #'zmq/sub-socket! (mock-fn socket-args "mock-subscription")
                     #'zmq/subscribe! (mock-fn subscribe-args "you-subscribed!")
                     #'zmq/close! (constantly "closed.")
                     #'zmq/send-more! (mock-fn send-more-args)
                     #'zmq/send! (mock-fn send-args)
                     #'ser/serialize (mock-fn-echo serialize-args)
                     #'ser/deserialize (mock-fn-echo deserialize-args)}
      f)))

(use-fixtures :each with-mock-functions)

(deftest test-publishing
  (testing "create publisher"
    (is (function? (publisher "tcp://foo-bar-baz:1234")))
    (is (= ["mock-context" "tcp://foo-bar-baz:1234"] @pub-socket-args))))

(deftest test-publishing-single-topic
  (testing "send topic and payload on single-topic publisher"
    (let [p (publisher "tcp://some-url:1234/someTopic")]
      (is (not (nil? (p "payload"))))
      (is (= ["mock-context" "tcp://some-url:1234"] @pub-socket-args))
      (is (= ["mock-publisher" "someTopic"] @send-more-args))
      (is (= ["mock-publisher" "payload"] @send-args))
      (is (= ["someTopic" "payload"] @serialize-args)))))

(deftest test-publishing-bind-failure
  (testing "creating a publisher that fails to bind should raise an exception."
    (with-redefs-fn {#'zmq/pub-socket! (fn [& args]
                                         (throw (Exception. "Surprise!")))}
      (fn
        []
        (is (thrown? java.lang.Exception (publisher "tcp://foo-bar-baz:1234")))))))

(deftest multi-topic-publishing
  (testing "send topic and payload on multi-topic publisher"
    (let [p (publisher "tcp://foo-bar-baz:1234")]
      (is (p "payload" "topic"))
      (is (= ["mock-publisher" "topic"] @send-more-args))
      (is (= ["mock-publisher" "payload"] @send-args))
      (is (= ["topic" "payload"] @serialize-args))))
  (testing "create multi-topic publisher and send payload without topic - should cause an error"
    (let [p (publisher "tcp://foo-bar-baz:1234")]
      (is (thrown? clojure.lang.ArityException (p "payload"))))))

(deftest test-subscription
  (testing "create subscription."
    (is (subscription "tcp://foo.bar:1234/topicName"))
    (is (= ["topicName"] @serialize-args))
    (is (= ["mock-context" "tcp://foo.bar:1234"] @socket-args))
    (is (= ["mock-subscription" "topicName"]) @subscribe-args))
  (testing "create all-topic subscription."
    (is (subscription "tcp://foo.bar:1234/*"))
    (is (= ["mock-context" "tcp://foo.bar:1234"] @socket-args))
    (is (= ["mock-subscription" ""]) @subscribe-args)))

(deftest test-pulling-messages-from-subscription
  (testing "should be able to pull messages off the subscription"
    (let [channel (async/to-chan ["topic" "first" "topic" "second"])]
      (with-redefs-fn {#'zmq/has-more? (mock-fn has-more-args true)
                       #'zmq/recv! (fn [& _] (async/<!! channel))}
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
                                      (swap! closed-items conj x))}
        (fn
          []
          (publisher "tcp://publish-addr:1234")
          (subscription "tcp://subscribe-address:1234")
          (close! :all)
          (is (= #{"mock-publisher" "mock-subscription" "mock-context"} (set @closed-items))))))))

(deftest cleanup-specific-items
  (testing "clean up a specific publisher"
    (let [closed-items (atom [])]
      (with-redefs-fn {#'zmq/close! (fn
                                      [x]
                                      (swap! closed-items conj x))}
        (fn
          []
          (let [p (publisher "tcp://hello:1234")]
            (close! p)
            (is (= ["mock-publisher"] @closed-items)))))))
  (testing "clean up a specific subscription"
    (let [closed-items (atom [])]
      (with-redefs-fn {#'zmq/close! (fn
                                      [x]
                                      (swap! closed-items conj x))}
        (fn
          []
          (let [s (subscription "tcp://hello:1234")]
            (close! s)
            (is (= ["mock-subscription"] @closed-items))))))))

(deftest cleanup-all-and-continue-working
  (testing "cleaning up :all and then creating a publisher or subscription should create a new context"
    (let [created-contexts (atom 0)]
      (with-redefs-fn {#'zmq/context! (fn
                                        []
                                        (swap! created-contexts inc)
                                        (str "mock-context-" @created-contexts))}
        (fn
          []
          (subscription "tcp://helloworld:1234/abc")
          (publisher "tcp://helloworld:1234/abc")
          (is (= 1 @created-contexts))
          (close! :all)
          (subscription "tcp://helloworld:1234/abc")
          (publisher "tcp://helloworld:1234/abc")
          (is (= 2 @created-contexts)))))))
