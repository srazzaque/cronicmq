(ns zeromq-clj.core-test
  (:require [clojure.test :refer :all]
            [zeromq-clj.core :refer :all]
            [zeromq-clj.zmq :as zmq]))

(defmacro create-mock-function-and-args [prefix]
  (let [function-name (symbol (str "mock-" prefix))
        catcher-name (symbol (str "mock-" prefix "-args"))]
    `(do
       (def ~catcher-name (atom []))
       (defn ~function-name
         [& args]
         (reset! ~catcher-name args)
         (str "mock-" ~prefix)))))

(doseq [mock '(context socket send send-more subscribe)]
  (create-mock-function-and-args mock))

;; TODO Refactor out the with-redefs-fn and use fixtures.

(deftest publishing

  (testing "create publisher with the implicit global context"
    (with-redefs-fn {#'zmq/socket mock-socket
                     #'zmq/context (fn [& _] "implicit-context")}
      (fn []
        (publisher "tcp://foo-bar-baz:1234")
        (is (= ["implicit-context" "tcp://foo-bar-baz:1234"] @mock-socket-args)))))

  (testing "create publisher with explicit context"
    (with-redefs-fn {#'zmq/socket mock-socket}
      (fn []
        (publisher "mock-context" "tcp://foo-bar-baz:1234")
        (is (= ["mock-context" "tcp://foo-bar-baz:1234"] @mock-socket-args)))))

  (testing "create single-topic publisher and send payload"
    (with-redefs-fn {#'zmq/socket mock-socket
                     #'zmq/context mock-context}
      (let [publisher (publisher "tcp://some-url:1234/someTopic")]
        (is (publisher "payload"))
        (is (= ["mock-socket" "someTopic"] @mock-send-more-args))
        (is (= ["mock-socket" "payload"] @mock-send-args)))))

  (testing "create multi-topic publisher and send topic and payload"
    (with-redefs-fn {#'zmq/socket (constantly "mock-socket")
                     #'zmq/send-more mock-send-more
                     #'zmq/send mock-send}
      (let [publisher (publisher "mock-context" "tcp://foo-bar-baz:1234")]
        (is (publisher "topic" "payload"))
        (is (= ["mock-socket" "topic"] @mock-send-more-args))
        (is (= ["mock-socket" "payload"] @mock-send-args)))))
  
  (testing "create multi-topic publisher and send payload without topic - should cause an error"))

(deftest subscription

  (testing "create topic subscription with implicit context."
    (with-redefs-fn {#'zmq/context (constantly "implicit-context")
                     #'zmq/socket mock-socket
                     #'zmq/subscribe mock-subscribe}
      (is (subscription "tcp://foo.bar:1234/topicName"))
      (is (= ["implicit-context" "tcp://foo.bar:1234"] @mock-socket-args))
      (is (= ["mock-subscription" "topicName"]) @mock-subscribe-args)))
  
  (testing "create all-topic subscription."
    (with-redefs-fn {#'zmq/context (constantly "implicit-context")
                     #'zmq/socket mock-socket
                     #'zmq/subscribe mock-subscribe}
      (is (subscription "tcp://foo.bar:1234/*"))
      (is (= ["implicit-context" "tcp://foo.bar:1234"] @mock-socket-args))
      (is (= ["mock-subscription" ""]) @mock-subscribe-args)))

  (testing "on-msg should perform the function it was provided with."
    (let [mock-message (promise)
          action-performed (atom false)]
      (with-redefs-fn {#'zmq/recv (fn [_] @mock-message)}
        (is (on-msg "mock-subsocket"
                    :do (fn [_] reset! action-performed true)
                    :while (constantly true)))
        (>!! mock-socket "foobar")
        (is (= "foobar" @action-performed)))))

  (testing "on-msg should continue pulling messages from the socket"
    (let [channel (chan)
          processed-messages (atom 0)]
      (with-redefs-fn {#'zmq/recv (<!! channel)}
        (is (on-msg "mock-subsocket"
                    :do (fn [_] (swap! processed-messages inc))
                    :until (constantly true)))
        (doseq [n (range 1 100)]
          (>!! channel "x")
          (is (= n @processed-messages)))))

  (testing "on-msg should stop performing the function provided if the :while condition returns false."
    (let [channel (chan)
          processed-messages (atom 0)
          process? (atom true)]
      (with-redefs-fn {#'zmq/recv (<!! channel)}
        (is (on-msg "mock-subsocket"
                    :do (fn [_] (swap! processed-messages inc))
                    :until #(@process?)))
        (>!! channel "x")
        (is (= 1 @processed-messages))
        (reset! process? false)
        (doseq [n (range 1 100)]
          (>!! channel "x")
          (is (= 1 @processed-messages)))))))

(deftest implicit-context

  ; This is the perfect candidate for a property-based test. Given any number of calls to any functions in the namespace,
  ; The zmq/context should only ever be called once
  (testing "implicit context should only ever be created once"
    (let [call-count (atom 0)])
    (with-redefs-fn {#'zmq/context (fn [& _] (swap! call-count inc))
                     #'zmq/socket mock-socket}
      (subscription "tcp://foo-bar-baz:1412/x")
      (subscription "tcp://foo-bar-baz:1413/x")
      (publisher "tcp://foo-bar-baz:1413/x")
      (publisher "tcp://foo-bar-baz:1413/")
      (is (= 1 @call-count)))))


(deftest context-cleanup

  (testing "calling close on a context should close all the sockets associated with it."
    (let [closed-items  (atom [])]
      (with-redefs-fn {#'zmq/close #(swap! closed-items conj %)
                       #'zmq/context mock-context}
        (let [ctx (context)]
          (publisher ctx "publish-addr")
          (subscription ctx "sub-addr")
          (close ctx)
          (is (= ["mock-context" "publish-addr" "sub-addr"] @closed-items))))))

  (testing "calling close on the implicit context should close all sockets created via the implicit context"
    (let [closed-items  (atom [])]
      (with-redefs-fn {#'zmq/close #(swap! closed-items conj %)
                       #'zmq/context mock-context}
        (let [ctx (context)]
          (publisher "tcp://publish-addr:1234")
          (subscription "tcp://subscribe-address:1234")
          (close)
          (is (= ["mock-context" "publish-addr" "sub-addr"] @closed-items)))))))
