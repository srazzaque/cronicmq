(ns zeromq-clj.core-test
  (:require [clojure.test :refer :all]
            [zeromq-clj.core :refer :all]))

(defmacro create-mock-function-and-catcher [prefix]
  (let [function-name (symbol (str "mock-" prefix))
        catcher-name (symbol (str "mock-" prefix "-catcher"))]
    `(do
       (def ~catcher-name (atom []))
       (defn ~function-name
         [& args]
         (reset! ~catcher-name args)
         (str "mock-" ~prefix)))))

(doseq [mock '(context create-socket send send-more)]
  (create-mock-function-and-catcher mock))

(deftest context-creation)

(deftest publishing
  (testing "creation of publisher"
    (with-redefs-fn {#'zmq/create-socket mock-create-socket}
      (fn []
        (create-publisher "mock-context" "tcp://foo-bar-baz:1234")
        (is (= ["mock-context" "tcp://foo-bar-baz:1234"] @mock-create-socket-catcher)))))
  (testing "publishing payload and topic"
    (with-redefs-fn {#'zmq/create-socket (constantly "mock-socket")
                     #'zmq/send-more mock-send-more
                     #'zmq/send mock-send}
      (let [publisher (create-publisher "mock-context" "tcp://foo-bar-baz:1234")]
        (publisher "topic" "payload")
        (is (= ["mock-socket" "topic"] @mock-send-more-catcher))
        (is (= ["mock-socket" "payload"] @mock-send-catcher))))))

(deftest subscription
  (testing "creation of a subscription socket should succeed."
    (is (= 0 1)))
  (testing "on-msg should perform the function it was provided with."
    (let [mock-message (promise)
          action-performed (atom false)]
      (with-redefs-fn {#'zmq/recv (fn [_] @mock-message)}
        (on-msg "mock-socket"
                :do (fn [_] reset! action-performed true)
                :while (constantly true))
        (>!! mock-socket "foobar")
        (is (= "foobar" @action-performed)))))
  (testing "on-msg should continue pulling messages from the socket"
    (is (= 0 1)))
  (testing "on-msg should stop performing the function provided if the :while condition returns false."
    (is (= 0 1))))

(deftest cleaning-up
  (testing "calling close on the context should close all the sockets associated with it."
    (let [closed-items  (atom [])]
      (with-redefs-fn {#'zmq/close #(swap! closed-items conj %)
                       #'zmq/context mock-context}
        (let [ctx (create-context)]
          (create-publisher ctx "publish-addr")
          (create-subscription ctx "sub-addr")
          (close ctx)
          (is (= ["mock-context" "publish-addr" "sub-addr"] @closed-items)))))))
